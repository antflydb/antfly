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

//! Private native index effects and their owned preparation. Receiving server
//! workers must independently obtain live metadata permission; these local
//! objects neither authenticate a coordinator nor enable a public producer.
const std = @import("std");
const jobs = @import("relational_index_owner_job.zig");
const inputs = @import("relational_index_inputs.zig");
const candidate_mod = @import("relational_index_candidate.zig");
const physical = @import("relational_index_catalog.zig");
const registry = @import("schema_registry.zig");
const schema_api = @import("../../schema/mod.zig");
const source_authority = @import("../source_authority.zig");
const Native = @import("receipt_position.zig").Native;
const private_claims = @import("relational_index_private_claims.zig");
const integrity = @import("relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;
const pages = @import("relational_index_jobs.zig");
const validation = @import("relational_index_validation.zig");
const release = @import("relational_index_release.zig");
const resources = @import("../resource_manager.zig");
const RequestContext = @import("../../api/operation.zig").RequestContext;
const Waiters = @import("../admission_waiter.zig").Fifo(void);
pub const envelope_version = 32;
pub const max_response_bytes = 32 * 1024;
pub const max_cancellation_wire_bytes = 32 * 1024;
pub const max_encoded_inputs = std.base64.standard.Encoder.calcSize(inputs.max_bytes);
pub const max_claim_wire_bytes = 6 * private_claims.max_bytes + 128 * 1024;
pub const max_control_wire_bytes = max_encoded_inputs + 32 * 1024;
pub const max_wire_bytes = @max(max_control_wire_bytes, max_claim_wire_bytes, pages.Page.PrivateCommand.max_wire_bytes);
pub const Manifest = inputs.Prepared;

pub const Admission = @import("relational_index_cancellation_intent.zig").Intent;

pub const Identity = @import("relational_index_owner_identity.zig").Identity;

/// Read-only canonical source observation. Absence and pending mirrors are
/// information only; neither can produce a source-batch signature or ACK.
pub const SourceRead = struct {
    binding: jobs.Binding,
    batch: ?private_claims.SourceStream.OwnedBatch = null,
    consumed: ?private_claims.SourceStream.Consumption = null,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn deinit(self: *SourceRead) void {
        if (self.batch) |*batch| batch.deinit();
        self.* = undefined;
    }

    pub fn requireIdentity(self: *const SourceRead, identity: Identity) !void {
        return self.requireIdentityAuthority(identity, .native);
    }

    pub fn requireRaftIdentity(self: *const SourceRead, identity: Identity) !void {
        return self.requireIdentityAuthority(identity, .raft);
    }

    fn requireIdentityAuthority(self: *const SourceRead, identity: Identity, authority: @import("relational_index_publication_progress.zig").Authority) !void {
        try identity.validate();
        _ = try self.binding.encode();
        const b = self.binding;
        if (b.phase != .reserved or b.authority != authority or b.ordinal != identity.ordinal or
            b.fence.owner_group_id != identity.owner_group_id or !b.fence.namespace.eql(identity.namespace) or
            !std.mem.eql(u8, &b.id, &identity.id) or !std.mem.eql(u8, &b.plan_digest, &identity.plan_digest) or
            !std.mem.eql(u8, &b.owner_digest, &identity.owner_digest) or self.replication_pending != (self.error_name != null) or
            (self.replication_pending and (authority == .raft or self.batch != null or self.consumed != null))) return error.InvalidPrivateIndexClaimInventory;
        if (self.consumed) |record| try record.requireFor(self.binding);
        if (self.batch) |batch| _ = try private_claims.SourceStream.Header.decode(&batch.header.bytes, self.binding, std.mem.readInt(u64, batch.header.bytes[104..112], .little));
        try requireSourcePrefix(self.binding, if (self.batch) |batch| batch.header else null, self.consumed);
    }

    pub fn digest(self: *const SourceRead) !integrity.Digest {
        const batch = if (self.batch) |*value| value else return error.InvalidPrivateIndexClaimReceipt;
        if (self.replication_pending or self.error_name != null or batch.header.bytes[184] == @backingInt(private_claims.SourceStream.State.failed)) return error.InvalidPrivateIndexClaimReceipt;
        try private_claims.delivery.View.fromBatch(batch).validate(self.binding);
        return sourceHeaderCommitment(self.binding, batch.header);
    }
};

/// Compact binary body in a bounded JSON control envelope. No per-row JSON or
/// Unicode conversion enters the logical tuple representation.
pub const SourceWire = struct {
    pub const max_payload_b64 = std.base64.standard.Encoder.calcSize(private_claims.SourceStream.max_payload_bytes);
    pub const max_wire_bytes = max_payload_b64 + 32 * 1024;
    binding: jobs.Binding,
    header: ?private_claims.SourceStream.Header = null,
    payload_b64: ?[]const u8 = null,
    consumed: ?private_claims.SourceStream.Consumption = null,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    /// Envelope checks alone do not validate the decoded source body. Remote
    /// consumers must decode before using rows or accepting source provenance.
    pub fn requireIdentity(self: SourceWire, identity: Identity) !void {
        return self.requireIdentityAuthority(identity, .native);
    }

    pub fn requireRaftIdentity(self: SourceWire, identity: Identity) !void {
        return self.requireIdentityAuthority(identity, .raft);
    }

    fn requireIdentityAuthority(self: SourceWire, identity: Identity, authority: @import("relational_index_publication_progress.zig").Authority) !void {
        if (self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexClaimInventory;
        if (self.error_name) |name| if (!std.mem.eql(u8, name, "ReplicationOutboxPending")) return error.InvalidPrivateIndexClaimInventory;
        try (SourceRead{ .binding = self.binding, .consumed = self.consumed, .replication_pending = self.replication_pending, .error_name = self.error_name }).requireIdentityAuthority(identity, authority);
        if ((self.header == null) != (self.payload_b64 == null) or (self.replication_pending and self.header != null)) return error.InvalidPrivateIndexClaimInventory;
        if (self.header) |header| _ = try private_claims.SourceStream.Header.decode(&header.bytes, self.binding, std.mem.readInt(u64, header.bytes[104..112], .little));
        try requireSourcePrefix(self.binding, self.header, self.consumed);
        if (self.payload_b64) |encoded| if (encoded.len > max_payload_b64) return error.InvalidPrivateIndexClaimInventory;
    }

    pub fn headerCommitment(self: SourceWire) !integrity.Digest {
        if (self.replication_pending or self.error_name != null or self.payload_b64 == null) return error.InvalidPrivateIndexClaimReceipt;
        return sourceHeaderCommitment(self.binding, self.header orelse return error.InvalidPrivateIndexClaimReceipt);
    }

    pub fn decode(self: SourceWire, alloc: Allocator, identity: Identity) !SourceRead {
        return self.decodeAuthority(alloc, identity, .native);
    }

    pub fn decodeRaft(self: SourceWire, alloc: Allocator, identity: Identity) !SourceRead {
        return self.decodeAuthority(alloc, identity, .raft);
    }

    fn decodeAuthority(self: SourceWire, alloc: Allocator, identity: Identity, authority: @import("relational_index_publication_progress.zig").Authority) !SourceRead {
        try self.requireIdentityAuthority(identity, authority);
        // The parsed wire arena may be released immediately after decoding.
        var result: SourceRead = .{ .binding = self.binding, .consumed = self.consumed, .replication_pending = self.replication_pending, .error_name = if (self.replication_pending) "ReplicationOutboxPending" else null };
        try result.requireIdentityAuthority(identity, authority);
        if ((self.header == null) != (self.payload_b64 == null) or (self.replication_pending and self.header != null)) return error.InvalidPrivateIndexClaimInventory;
        if (self.payload_b64) |encoded| {
            if (encoded.len > max_payload_b64) return error.InvalidPrivateIndexClaimInventory;
            const decoder = std.base64.standard.Decoder;
            const size = decoder.calcSizeForSlice(encoded) catch return error.InvalidPrivateIndexClaimInventory;
            if (size > private_claims.SourceStream.max_payload_bytes) return error.InvalidPrivateIndexClaimInventory;
            const body = try alloc.alloc(u8, size);
            errdefer alloc.free(body);
            decoder.decode(body, encoded) catch return error.InvalidPrivateIndexClaimInventory;
            result.batch = try private_claims.SourceStream.OwnedBatch.takePayload(alloc, self.binding, self.header.?, body);
        }
        return result;
    }
};

/// Wire observations cannot splice a valid old consumed prefix into another
/// first-pending batch. This remains structural data, not a source signature.
fn requireSourcePrefix(binding: jobs.Binding, header: ?private_claims.SourceStream.Header, consumed: ?private_claims.SourceStream.Consumption) !void {
    const batch = header orelse return;
    const ordinal = std.mem.readInt(u64, batch.bytes[104..112], .little);
    if (consumed) |record| {
        try record.requireFor(binding);
        const position = try batch.orderedPosition();
        const prior = try record.header.orderedPosition();
        const sequence = switch (position) {
            .native => |value| if (binding.authority == .native) value.sequence else return error.InvalidControlReceiptPosition,
            .raft => |value| if (binding.authority == .raft) value.index else return error.InvalidControlReceiptPosition,
        };
        if (record.accepted.ordinal == std.math.maxInt(u64) or ordinal != record.accepted.ordinal + 1 or record.accepted.state != .building or
            !std.mem.eql(u8, batch.bytes[256..288], &record.accepted.batch_digest) or !std.mem.eql(u8, batch.bytes[40..72], &record.accepted.scope_digest) or
            sequence <= record.accepted.source_sequence or position.term() < prior.term()) return error.InvalidPrivateIndexClaimInventory;
    } else if (ordinal != 1 or !std.mem.allEqual(u8, batch.bytes[256..288], 0)) return error.InvalidPrivateIndexClaimInventory;
}

fn sourceHeaderCommitment(binding: jobs.Binding, header: private_claims.SourceStream.Header) !integrity.Digest {
    _ = try private_claims.SourceStream.Header.decode(&header.bytes, binding, std.mem.readInt(u64, header.bytes[104..112], .little));
    if (binding.phase != .reserved or header.bytes[184] == @backingInt(private_claims.SourceStream.State.failed)) return error.InvalidPrivateIndexClaimReceipt;
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/private-unique-source-batch/v1");
    state.update(&try binding.encode());
    state.update(&header.digest());
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub const Command = struct {
    effect: jobs.NativeEffect,
    binding: jobs.Binding,
    inputs_b64: ?[]const u8 = null,

    pub fn validate(self: Command) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .native or self.binding.phase != .fencing or
            ((self.effect == .admitted) != (self.inputs_b64 != null))) return error.InvalidPrivateIndexOwnerJob;
        if (self.inputs_b64) |bytes| if (bytes.len == 0 or bytes.len > max_encoded_inputs) return error.InvalidPrivateIndexInputs;
    }
};

/// Receiving workers must independently bind the actual hash-owner placement
/// and live metadata permission. The kernel checks its persisted job, range,
/// cancellation and exact-cut receipt; this body is not an RPC capability.
pub const ClaimCommand = struct {
    binding: jobs.Binding,
    claims: []const integrity.Claim,

    /// Shared by the compiled owner and normalized replay decoder. Reject a
    /// 129th record before parsing/allocating it; a wire-size ceiling alone
    /// otherwise admits thousands of tiny structs before semantic validation.
    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !ClaimCommand {
        const wire = try std.json.innerParse(struct { binding: jobs.Binding, claims: BoundedClaims }, alloc, source, options);
        return .{ .binding = wire.binding, .claims = wire.claims.items };
    }

    /// Shared bounded decoding for physical control and identity-only RPCs.
    pub const BoundedClaims = struct {
        items: []const integrity.Claim,

        pub fn validate(self: BoundedClaims) !void {
            if (self.items.len == 0 or self.items.len > private_claims.max_claims) return error.InvalidPrivateIndexClaimInventory;
            var bytes: usize = 0;
            for (self.items) |claim| {
                if (claim.state != .live or claim.action != .restrict or claim.target_tuple != null or
                    claim.tuple.len == 0 or claim.parent_table.len == 0 or claim.parent_key.len == 0 or
                    !std.mem.allEqual(u8, &claim.action_id, 0)) return error.InvalidPrivateIndexClaimInventory;
                bytes = std.math.add(usize, bytes, private_claims.claim_entry_overhead) catch return error.TransactionTooLarge;
                for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| bytes = std.math.add(usize, bytes, field.len) catch return error.TransactionTooLarge;
            }
            if (bytes > private_claims.max_bytes) return error.TransactionTooLarge;
        }

        pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !BoundedClaims {
            if (try source.next() != .array_begin) return error.UnexpectedToken;
            var items: std.ArrayList(integrity.Claim) = .empty;
            var bytes: usize = 0;
            var limited = options;
            limited.max_value_len = @min(options.max_value_len.?, private_claims.max_bytes);
            while (try source.peekNextTokenType() != .array_end) {
                if (items.items.len >= private_claims.max_claims) return error.LengthMismatch;
                const claim = try std.json.innerParse(integrity.Claim, alloc, source, limited);
                if (claim.state != .live or claim.action != .restrict or claim.target_tuple != null or
                    !std.mem.allEqual(u8, &claim.action_id, 0)) return error.UnexpectedToken;
                bytes = std.math.add(usize, bytes, private_claims.claim_entry_overhead) catch return error.Overflow;
                for ([_][]const u8{ claim.tuple, claim.parent_table, claim.parent_key }) |field| bytes = std.math.add(usize, bytes, field.len) catch return error.Overflow;
                if (bytes > private_claims.max_bytes) return error.LengthMismatch;
                try items.append(alloc, claim);
            }
            _ = try source.next();
            if (items.items.len == 0) return error.LengthMismatch;
            return .{ .items = try items.toOwnedSlice(alloc) };
        }
    };

    pub fn validate(self: ClaimCommand) !void {
        _ = try self.binding.encode();
        if (self.binding.phase != .reserved) return error.InvalidPrivateIndexClaimInventory;
        try (BoundedClaims{ .items = self.claims }).validate();
        for (self.claims) |claim| {
            if (claim.schema_version != self.binding.physical_candidate.schema_version or
                claim.tuple.len == 0 or claim.parent_table.len == 0 or claim.parent_key.len == 0) return error.InvalidPrivateIndexClaimInventory;
        }
    }

    /// Canonical set commitment: bounded stack sorting, no allocation or
    /// JSON encoding. Identical retries/regrouping preserve claim identity.
    pub fn digest(self: ClaimCommand) !integrity.Digest {
        return (try self.commitment()).digest;
    }

    pub fn commitment(self: ClaimCommand) !struct { digest: integrity.Digest, count: usize } {
        try self.validate();
        const Item = struct { tuple: integrity.Digest, value: integrity.Digest };
        var items: [private_claims.max_claims]Item = undefined;
        for (self.claims, items[0..self.claims.len]) |claim, *item| {
            item.tuple = integrity.hash(claim.tuple);
            item.value = memberDigest(claim);
        }
        const active = items[0..self.claims.len];
        std.mem.sort(Item, active, {}, struct {
            fn less(_: void, a: Item, b: Item) bool {
                return std.mem.order(u8, &a.tuple, &b.tuple) == .lt;
            }
        }.less);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-claim-set/v1");
        state.update(&try self.binding.encode());
        var previous: ?Item = null;
        var count: usize = 0;
        for (active) |item| {
            if (previous) |prior| if (std.mem.eql(u8, &prior.tuple, &item.tuple)) {
                if (!std.mem.eql(u8, &prior.value, &item.value)) return error.UniqueConstraintViolation;
                continue;
            };
            state.update(&item.tuple);
            state.update(&item.value);
            count += 1;
            previous = item;
        }
        var result: integrity.Digest = undefined;
        state.final(&result);
        return .{ .digest = result, .count = count };
    }

    /// Logical member identity shared by source contributions and eventual
    /// target inventory seals. Independent of physical claim-value encoding.
    pub fn memberDigest(claim: integrity.Claim) integrity.Digest {
        return claim.memberDigest();
    }
};

/// Recover only a previously committed publication effect. No physical binding,
/// candidate digest or replay position is accepted from a receipt caller.
pub const ReceiptRequest = struct {
    identity: Identity,
    effect: enum { fenced, reserved },
};

/// Exactly one owner effect per transaction. This unpublished PR format has
/// no legacy parallel-field envelopes: mixed effects are not representable.
pub const Effect = union(enum) {
    control: Command,
    cancel_control: Command,
    cancel_intent: Admission,
    cancel_cleanup: jobs.Binding,
    cancel_complete: jobs.Binding,
    cancel_release: release.CancellationIntent,
    claims: ClaimCommand,
    build: pages.Page.PrivateCommand,
    consume: ConsumeCommand,
    inventory_validation: private_claims.InventoryValidation.Command,
    generation_validation: validation.Command,
    validation_seal: validation.SealCommand,
    install: validation.InstallCommand,
    release: release.Intent,
    release_gc: release.Gc.Command,

    pub fn namespace(self: Effect) @FieldType(Native, "namespace") {
        return switch (self) {
            .control, .cancel_control => |value| value.binding.fence.namespace,
            .claims => |value| value.binding.fence.namespace,
            .consume => |value| value.binding.fence.namespace,
            .inventory_validation => |value| value.binding.fence.namespace,
            .generation_validation => |value| value.binding.fence.namespace,
            .validation_seal => |value| value.binding.fence.namespace,
            .install => |value| value.validation.command.binding.fence.namespace,
            .release => |value| value.installed.command.validation.command.binding.fence.namespace,
            .cancel_release => |value| value.canceled.binding.fence.namespace,
            .release_gc => |value| value.record.intent.installed.command.validation.command.binding.fence.namespace,
            .build => |value| value.binding.fence.namespace,
            .cancel_intent => |value| value.fence.namespace,
            .cancel_cleanup, .cancel_complete => |value| value.fence.namespace,
        };
    }

    pub fn validate(self: Effect) !void {
        switch (self) {
            .control, .cancel_control => |value| try value.validate(),
            .claims => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .consume => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .inventory_validation => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .generation_validation => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .validation_seal => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .install => |value| {
                try value.validate();
                if (value.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .release => |value| {
                try value.validate();
                if (value.installed.position != .native) return error.InvalidControlReceiptPosition;
            },
            .cancel_release => |value| {
                try value.validate();
                if (value.canceled.position != .native) return error.InvalidControlReceiptPosition;
            },
            .release_gc => |value| {
                try value.validate();
                if (value.record.position != .native) return error.InvalidControlReceiptPosition;
            },
            .build => |value| {
                try value.validate();
                if (value.binding.authority != .native) return error.InvalidControlReceiptPosition;
            },
            .cancel_intent => |value| try value.validate(),
            .cancel_cleanup, .cancel_complete => |value| {
                _ = try value.encode();
                if (value.authority != .native or value.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
            },
        }
    }

    pub fn wireLimit(self: Effect) usize {
        return switch (self) {
            .control, .cancel_control => max_control_wire_bytes,
            .claims => max_claim_wire_bytes,
            .build => pages.Page.PrivateCommand.max_wire_bytes,
            .inventory_validation => private_claims.InventoryValidation.max_wire_bytes,
            .generation_validation => validation.max_wire_bytes,
            .validation_seal => validation.Seal.max_wire_bytes,
            .install => validation.Installed.max_wire_bytes,
            .release => release.Record.max_wire_bytes,
            .release_gc => release.Gc.max_wire_bytes,
            else => max_cancellation_wire_bytes,
        };
    }
};

pub const Frame = struct {
    effect: Effect,
    position: Native,

    pub fn validate(self: Frame) !void {
        try self.effect.validate();
        try self.position.validate();
        if (!self.position.namespace.eql(self.effect.namespace())) return error.IdentityNamespaceMismatch;
    }
};

/// Durable intent progress only: this type deliberately has no publication
/// receipt. A pending mirror must not become a canceled owner acknowledgement.
pub const CancellationOutcome = struct {
    intent: Admission,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
};

pub const CleanupOutcome = struct {
    binding: jobs.Binding,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
};

pub const Outcome = struct {
    binding: jobs.Binding,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,

    /// Provider bytes are not a signing authority. Validate the exact command
    /// identity and any returned proof before retaining an applying-root cut.
    pub fn validateFor(self: Outcome, command: Command) !void {
        try command.validate();
        var actual = self.binding;
        actual.phase = .fencing;
        const actual_bytes = actual.encode() catch return error.InvalidPrivateIndexNativeReceipt;
        const expected_bytes = try command.binding.encode();
        if (!std.mem.eql(u8, &actual_bytes, &expected_bytes)) return error.InvalidPrivateIndexNativeReceipt;
        try self.validateCut(command.effect);
    }

    pub fn validateRecovery(self: Outcome, request: ReceiptRequest) !void {
        try request.identity.validate();
        const binding = self.binding;
        const identity = request.identity;
        if (!std.mem.eql(u8, &binding.id, &identity.id) or
            !std.mem.eql(u8, &binding.plan_digest, &identity.plan_digest) or
            !std.mem.eql(u8, &binding.owner_digest, &identity.owner_digest) or
            binding.ordinal != identity.ordinal or binding.fence.owner_group_id != identity.owner_group_id or
            !binding.fence.namespace.eql(identity.namespace)) return error.InvalidPrivateIndexNativeReceipt;
        try self.validateCut(switch (request.effect) {
            .fenced => .fenced,
            .reserved => .reserved,
        });
    }

    fn validateCut(self: Outcome, effect: jobs.NativeEffect) !void {
        _ = self.binding.encode() catch return error.InvalidPrivateIndexNativeReceipt;
        self.position.validate() catch return error.InvalidPrivateIndexNativeReceipt;
        if (self.binding.authority != .native or !self.position.namespace.eql(self.binding.fence.namespace) or
            (self.replication_pending and self.error_name == null)) return error.InvalidPrivateIndexNativeReceipt;
        if (self.receipt) |receipt| {
            if (self.replication_pending or self.error_name != null) return error.InvalidPrivateIndexNativeReceipt;
            try jobs.validateNativeReceipt(self.binding, effect, self.position, receipt);
        }
    }
};

/// Physical progress only, not a canceled publication receipt or source cut.
pub const CleanupMaintenanceOutcome = struct {
    advanced: bool,
    artifacts_retired: bool,
};

pub const ClaimCleanupMaintenanceOutcome = struct {
    advanced: bool,
    claims_retired: bool,
};

pub const ClaimOutcome = struct {
    binding: jobs.Binding,
    claims_digest: integrity.Digest,
    position: Native,
    duplicate: bool,
    claims_added: usize = 0,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    /// Physical delivery acknowledgement only, never a publication signature.
    /// Check a decoded provider result before a receiver can use its cut.
    pub fn validateFor(self: ClaimOutcome, command: ClaimCommand) !void {
        const expected_set = try command.commitment();
        const actual = self.binding.encode() catch return error.InvalidPrivateIndexClaimReceipt;
        const expected = try command.binding.encode();
        self.position.validate() catch return error.InvalidPrivateIndexClaimReceipt;
        if (command.binding.authority != .native or !std.mem.eql(u8, &actual, &expected) or
            !std.mem.eql(u8, &self.claims_digest, &expected_set.digest) or
            !self.position.namespace.eql(command.binding.fence.namespace) or
            self.claims_added > expected_set.count or
            self.duplicate != (self.claims_added == 0) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexClaimReceipt;
    }

    /// Sign only committed, fully mirrored delivery. This domain cannot be
    /// confused with a publication/validation receipt.
    pub fn deliveryDigest(self: ClaimOutcome) !integrity.Digest {
        _ = try self.binding.encode();
        try self.position.validate();
        if (self.binding.authority != .native or !self.position.namespace.eql(self.binding.fence.namespace) or
            self.replication_pending or self.error_name != null or std.mem.allEqual(u8, &self.claims_digest, 0))
            return error.InvalidPrivateIndexClaimReceipt;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-delivery/v1");
        state.update(&try self.binding.encode());
        state.update(&self.claims_digest);
        state.update(&try (@import("receipt_position.zig").Position{ .native = self.position }).encode());
        var result: integrity.Digest = undefined;
        state.final(&result);
        return result;
    }
};

/// Shared signed-delivery shape. Native engine outcomes convert explicitly;
/// recovery reads retain Raft term/index, never a native-clock alias.
pub const DeliveredClaims = struct {
    binding: jobs.Binding,
    claims_digest: integrity.Digest,
    position: @import("receipt_position.zig").Position,
    duplicate: bool,
    claims_added: usize = 0,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn fromNative(value: ClaimOutcome) !@This() {
        _ = try value.deliveryDigest();
        return .{ .binding = value.binding, .claims_digest = value.claims_digest, .position = .{ .native = value.position }, .duplicate = value.duplicate, .claims_added = value.claims_added };
    }
    pub fn validateFor(self: @This(), command: ClaimCommand) !void {
        const expected = try command.commitment();
        if (!std.mem.eql(u8, &try self.binding.encode(), &try command.binding.encode()) or !std.mem.eql(u8, &self.claims_digest, &expected.digest) or
            self.claims_added > expected.count or self.duplicate != (self.claims_added == 0)) return error.InvalidPrivateIndexClaimReceipt;
        _ = try self.deliveryDigest();
    }
    pub fn deliveryDigest(self: @This()) !integrity.Digest {
        _ = try self.binding.encode();
        try self.position.validate();
        try self.position.requireNamespace(self.binding.fence.namespace);
        if ((self.position == .native) != (self.binding.authority == .native) or self.binding.phase != .reserved or
            self.replication_pending or self.error_name != null or std.mem.allEqual(u8, &self.claims_digest, 0)) return error.InvalidPrivateIndexClaimReceipt;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-unique-delivery/v1");
        state.update(&try self.binding.encode());
        state.update(&self.claims_digest);
        state.update(&try self.position.encode());
        var result: integrity.Digest = undefined;
        state.final(&result);
        return result;
    }
};

pub const ConsumeCommand = struct {
    binding: jobs.Binding,
    accepted: private_claims.SourceStream.Accepted,
    pub fn validate(self: ConsumeCommand) !void {
        _ = try self.binding.encode();
        try self.accepted.validate();
        var binding_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(&try self.binding.encode(), &binding_digest, .{});
        if (self.binding.phase != .reserved or
            !std.mem.eql(u8, &self.accepted.binding_digest, &binding_digest)) return error.InvalidPrivateIndexClaimReceipt;
    }
};

pub const ConsumeOutcome = struct {
    binding: jobs.Binding,
    accepted: private_claims.SourceStream.Accepted,
    record: ?private_claims.SourceStream.Consumption,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    pub fn validateFor(self: ConsumeOutcome, command: ConsumeCommand) !void {
        try command.validate();
        if (!std.mem.eql(u8, &try self.binding.encode(), &try command.binding.encode()) or
            !std.mem.eql(u8, &try self.accepted.encode(), &try command.accepted.encode()) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
        const record = self.record orelse return error.InvalidPrivateIndexNativeReceipt;
        try record.requireFor(self.binding);
        if (record.position != .native or !std.mem.eql(u8, &record.accepted.decision_digest, &self.accepted.decision_digest) or !std.meta.eql(record.position.native, self.position)) return error.InvalidPrivateIndexNativeReceipt;
    }
};

/// Off-lock canonical preparation and the same native apply/outbox machinery
/// as build/claims. Metadata authorization belongs to the receiving server.
pub const PreparedConsumption = struct {
    pub const ApplyOutcome = ConsumeOutcome;
    alloc: Allocator,
    command: ConsumeCommand,
    prepared: private_claims.SourceStream.PreparedConsumption,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,
    context: RequestContext = .{},
    pub fn init(alloc: Allocator, txn: anytype, command: ConsumeCommand) !PreparedConsumption {
        return initControlled(alloc, txn, command, .{});
    }
    pub fn initControlled(alloc: Allocator, txn: anytype, command: ConsumeCommand, context: RequestContext) !PreparedConsumption {
        try context.ensureActive();
        try command.validate();
        try PreparedBuild.verifyRecovery(txn, command.binding);
        var result: PreparedConsumption = .{ .alloc = alloc, .command = command, .context = context, .prepared = try private_claims.SourceStream.PreparedConsumption.init(alloc, txn, command.binding, command.accepted) };
        errdefer result.deinit();
        try context.ensureActive();
        return result;
    }
    pub fn deinit(self: *PreparedConsumption) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.prepared.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedConsumption) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const PreparedConsumption) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedConsumption, position: Native) !void {
        try (Frame{ .effect = .{ .consume = self.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedConsumption, position: Native) ConsumeOutcome {
        return .{ .binding = self.command.binding, .accepted = self.command.accepted, .record = null, .position = position, .duplicate = true };
    }
    pub fn stage(self: *PreparedConsumption, _: anytype, txn: anytype, replay: ?Native) !ConsumeOutcome {
        // The serialized apply lock may have waited past ingress cancellation.
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try self.context.ensureActive();
        try PreparedBuild.verifyRecovery(txn, self.command.binding);
        try self.context.ensureActive();
        const result = try self.prepared.stage(txn, replay);
        return .{ .binding = self.command.binding, .accepted = self.command.accepted, .record = result.record, .position = result.record.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *PreparedConsumption, _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.context.ensureActive();
        if (self.command.binding.authority != .raft or owner_group == 0 or owner_group != self.command.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        try PreparedBuild.verifyRecovery(txn, self.command.binding);
        try self.context.ensureActive();
        _ = try self.prepared.stageOrdered(txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const PreparedConsumption) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedConsumption, _: anytype, _: *ConsumeOutcome) !void {}
    pub fn preparePayload(self: *PreparedConsumption) !void {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "consume", self.command, max_cancellation_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedConsumption, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

pub const PreparedClaims = struct {
    pub const ApplyOutcome = ClaimOutcome;
    alloc: Allocator,
    batch: private_claims.Prepared,
    command: ClaimCommand,
    claims_digest: integrity.Digest,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,
    context: RequestContext = .{},

    pub fn init(alloc: Allocator, txn: anytype, command: ClaimCommand) !PreparedClaims {
        try command.validate();
        var batch = try private_claims.Prepared.init(alloc, txn, command.binding, command.claims);
        errdefer batch.deinit();
        const owned = try batch.arena.allocator().alloc(integrity.Claim, batch.entries.len);
        for (owned, batch.entries) |*claim, entry| claim.* = try integrity.Claim.decode(&entry.address.claimKey(), entry.value);
        return .{ .alloc = alloc, .batch = batch, .command = .{ .binding = command.binding, .claims = owned }, .claims_digest = try command.digest() };
    }

    pub fn deinit(self: *PreparedClaims) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.batch.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedClaims) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const PreparedClaims) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedClaims, position: Native) !void {
        try (Frame{ .effect = .{ .claims = self.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedClaims, position: Native) ClaimOutcome {
        return .{ .binding = self.command.binding, .claims_digest = self.claims_digest, .position = position, .duplicate = true };
    }
    pub fn stage(self: *PreparedClaims, core: anytype, txn: anytype, replay: ?Native) !ClaimOutcome {
        try self.context.ensureActive();
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        // Actual receiving range, checked under apply exclusion; no caller-set
        // ownership flag or row owner's routing assumption is accepted.
        for (self.batch.entries) |entry| if (!core.byteRange().contains(&entry.address.routing)) return error.KeyOutOfRange;
        const result = try self.batch.stageOrdered(txn, replay);
        return .{ .binding = self.command.binding, .claims_digest = self.claims_digest, .position = result.position, .duplicate = result.duplicate, .claims_added = result.claims_added };
    }
    pub fn stageRaft(self: *PreparedClaims, core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.context.ensureActive();
        if (self.command.binding.authority != .raft or owner_group == 0 or owner_group != self.command.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        for (self.batch.entries) |value| if (!core.byteRange().contains(&value.address.routing)) return error.KeyOutOfRange;
        _ = try self.batch.stageAt(txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const PreparedClaims) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedClaims, _: anytype, _: *ClaimOutcome) !void {}
    pub fn preparePayload(self: *PreparedClaims) !void {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "claims", self.command, max_claim_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedClaims, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// A build page is durable progress, not a UNIQUE-validation receipt. One
/// retained checkpoint bounds retry state regardless of table size or churn.
pub const BuildOutcome = struct {
    binding: jobs.Binding,
    page_digest: [32]u8,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn validateFor(self: BuildOutcome, command: pages.Page.PrivateCommand) !void {
        try command.validate();
        try self.position.validate();
        if (command.binding.authority != .native or !std.mem.eql(u8, &try self.binding.encode(), &try command.binding.encode()) or
            !std.mem.eql(u8, &self.page_digest, &try command.digest()) or
            !self.position.namespace.eql(command.binding.fence.namespace) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
    }
};

/// A receiving worker supplies identity plus an observed binding commitment,
/// never physical allocation choices, schema inputs or a replay position.
/// This is a CAS precondition, not metadata authorization.
pub const BuildRequest = struct {
    identity: Identity,
    binding_digest: [32]u8,
    budget: pages.Budget = .{},
    target_expectation: ?private_claims.TargetExpectation = null,

    pub fn init(identity: Identity, binding: jobs.Binding, budget: pages.Budget) !BuildRequest {
        const result: BuildRequest = .{ .identity = identity, .binding_digest = try bindingDigest(binding), .budget = budget };
        try result.requireBinding(binding);
        return result;
    }
    pub fn initRaft(identity: Identity, binding: jobs.Binding, budget: pages.Budget) !BuildRequest {
        const result: BuildRequest = .{ .identity = identity, .binding_digest = try bindingDigest(binding), .budget = budget };
        try result.requireRaftBinding(binding);
        return result;
    }
    pub fn validate(self: BuildRequest) !void {
        try self.identity.validate();
        try self.budget.validate();
        if (std.mem.allEqual(u8, &self.binding_digest, 0)) return error.InvalidPrivateIndexOwnerJob;
        if (self.target_expectation) |expected| {
            try expected.validate();
            if (!std.mem.eql(u8, &expected.binding_digest, &self.binding_digest)) return error.PrivateIndexOwnerJobChanged;
        }
    }
    pub fn requireBinding(self: BuildRequest, binding: jobs.Binding) !void {
        return self.requireBindingAuthority(binding, .native);
    }
    pub fn requireRaftBinding(self: BuildRequest, binding: jobs.Binding) !void {
        return self.requireBindingAuthority(binding, .raft);
    }
    fn requireBindingAuthority(self: BuildRequest, binding: jobs.Binding, authority: @import("relational_index_publication_progress.zig").Authority) !void {
        try self.validate();
        if (self.target_expectation) |expected| try expected.requireOwnerBinding(binding);
        const identity = self.identity;
        if (binding.phase != .reserved or binding.authority != authority or
            !std.mem.eql(u8, &binding.id, &identity.id) or !std.mem.eql(u8, &binding.plan_digest, &identity.plan_digest) or
            !std.mem.eql(u8, &binding.owner_digest, &identity.owner_digest) or binding.ordinal != identity.ordinal or
            !binding.fence.namespace.eql(identity.namespace) or binding.fence.owner_group_id != identity.owner_group_id or
            !std.mem.eql(u8, &try bindingDigest(binding), &self.binding_digest)) return error.PrivateIndexOwnerJobChanged;
    }
    fn bindingDigest(binding: jobs.Binding) ![32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(&try binding.encode(), &result, .{});
        return result;
    }
};

/// Physical terminal progress only. Complete claims and publication still
/// require their independent protocols; this cannot carry a publication ACK.
pub const BuildCompletion = struct {
    binding: jobs.Binding,
    state: enum { ready, failed },
    failure: pages.Failure,
    rows_scanned: u64,
    progress_digest: [32]u8,
    claim_batches_pending: u64 = 0,
    claim_bytes_pending: u64 = 0,
    position: Native,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn validateFor(self: BuildCompletion, request: BuildRequest) !void {
        if (request.target_expectation != null) return error.InvalidPrivateIndexNativeReceipt;
        try request.requireBinding(self.binding);
        try self.position.validate();
        if (!self.position.namespace.eql(request.identity.namespace) or
            std.mem.allEqual(u8, &self.progress_digest, 0) or
            self.claim_batches_pending > private_claims.SourceStream.max_pending or
            self.claim_bytes_pending > private_claims.SourceStream.max_pending_bytes or
            (self.claim_batches_pending == 0) != (self.claim_bytes_pending == 0) or
            (self.state == .failed) != (self.failure != .none) or
            self.replication_pending != (self.error_name != null)) return error.InvalidRelationalIndexProgress;
    }
};

pub const BuildResponse = union(enum) {
    advanced: BuildOutcome,
    complete: BuildCompletion,
    inventory: InventoryOutcome,
    generation: GenerationOutcome,
    audit: AuditOutcome,
    validated: ValidatedOutcome,

    pub fn physicalAudit(self: @This()) ?AuditOutcome {
        return switch (self) {
            .audit => |value| value,
            .validated => |value| value.physical,
            else => null,
        };
    }

    pub fn validateFor(self: BuildResponse, request: BuildRequest) !void {
        switch (self) {
            .validated => |value| try value.validateFor(request),
            .audit => |value| try value.validateFor(request),
            .inventory => |value| try value.validateFor(request),
            .generation => |value| try value.validateFor(request),
            .complete => |value| try value.validateFor(request),
            .advanced => |value| {
                if (request.target_expectation != null) return error.InvalidPrivateIndexNativeReceipt;
                try request.requireBinding(value.binding);
                try value.position.validate();
                if (!value.position.namespace.eql(request.identity.namespace) or std.mem.allEqual(u8, &value.page_digest, 0) or
                    value.replication_pending != (value.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
            },
        }
    }
};

/// Host-selected signing authority, sent only across the private compiled
/// boundary. The live DB supplies its separate physical incarnation. Remote
/// control requests never select this filesystem context.
pub const AuditRoot = struct {
    pub const max_wire_bytes = 6 * std.Io.Dir.max_path_bytes + 256;
    root_dir: []const u8,
    root_incarnation: u128,
    pub fn validate(self: @This()) !void {
        if (self.root_incarnation == 0 or self.root_dir.len == 0 or self.root_dir.len > std.Io.Dir.max_path_bytes or std.mem.indexOfScalar(u8, self.root_dir, 0) != null) return error.InvalidPrivateIndexNativeReceipt;
    }
};
test "relational index build pages audit scope retains escaped path bytes within the scalar limit" {
    const alloc = std.testing.allocator;
    const path = try alloc.alloc(u8, std.Io.Dir.max_path_bytes);
    defer alloc.free(path);
    @memset(path, 1);
    const value: AuditRoot = .{ .root_dir = path, .root_incarnation = 1 };
    try value.validate();
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(json);
    try std.testing.expect(json.len > 5 * path.len and json.len <= AuditRoot.max_wire_bytes);
    var parsed = try std.json.parseFromSlice(AuditRoot, alloc, json, .{ .ignore_unknown_fields = false, .max_value_len = AuditRoot.max_wire_bytes });
    defer parsed.deinit();
    try parsed.value.validate();
    try std.testing.expectEqualSlices(u8, path, parsed.value.root_dir);
}
/// Unsigned progress only. The local checkpoint digest is not a portable
/// publication certificate and never substitutes for metadata proof joining.
pub const AuditOutcome = struct {
    binding: jobs.Binding,
    expected: private_claims.TargetExpectation,
    audit: @import("relational_index_audit_checkpoint.zig").Summary,
    pub fn logicalCommand(self: @This()) !validation.SealCommand {
        _ = try self.readyDigest();
        return .{ .binding = self.binding, .expected = self.expected, .generation_digest = self.audit.generation_canonical_digest.?, .inventory_digest = self.audit.inventory_canonical_digest, .source_members = self.audit.source_members.?, .generation_position = self.audit.canonical_position.? };
    }
    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        try request.requireBinding(self.binding);
        if (!std.mem.eql(u8, &try self.expected.digest(), &try (request.target_expectation orelse return error.InvalidPrivateIndexNativeReceipt).digest())) return error.InvalidPrivateIndexNativeReceipt;
        try self.audit.validateFor(self.expected, self.audit.root_incarnation);
        if (self.audit.canonical_position) |position| {
            if (position != .native) return error.InvalidPrivateIndexNativeReceipt;
            position.requireNamespace(self.binding.fence.namespace) catch return error.InvalidPrivateIndexNativeReceipt;
        }
    }
    pub fn validateForRaft(self: @This(), request: BuildRequest) !void {
        try request.requireRaftBinding(self.binding);
        if (!std.mem.eql(u8, &try self.expected.digest(), &try (request.target_expectation orelse return error.InvalidRelationalIndexValidation).digest())) return error.InvalidRelationalIndexValidation;
        try self.audit.validateFor(self.expected, self.audit.root_incarnation);
        if (self.audit.canonical_position) |position| if (position != .raft) return error.InvalidControlReceiptPosition;
    }
    /// Physical witness identity only, deliberately distinct from a logical
    /// publication receipt. It changes when the physical DB is replaced.
    pub fn readyDigest(self: @This()) !integrity.Digest {
        try self.expected.requireOwnerBinding(self.binding);
        try self.audit.validateFor(self.expected, self.audit.root_incarnation);
        if (self.audit.inventory.phase != .ready or self.audit.generation == null or self.audit.generation.?.phase != .ready) return error.PrivateIndexTargetValidationRequired;
        const position = self.audit.canonical_position.?;
        if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
        try position.requireNamespace(self.binding.fence.namespace);
        var buffer: [16 * 1024]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buffer);
        std.json.Stringify.value(self, .{}, &writer) catch return error.InvalidPrivateIndexNativeReceipt;
        var digest = std.crypto.hash.Blake3.init(.{});
        digest.update("antfly/private-index-complete-physical-audit/v3");
        digest.update(buffer[0..writer.end]);
        var result: integrity.Digest = undefined;
        digest.final(&result);
        return result;
    }
};

/// The stable logical cut and replaceable physical witness travel together,
/// but are authenticated in distinct domains. Neither is a metadata ACK.
pub const ValidatedOutcome = struct {
    physical: AuditOutcome,
    logical: ValidationSealOutcome,

    pub fn validateFor(self: @This(), request: BuildRequest) !void {
        try self.physical.validateFor(request);
        _ = try self.physical.readyDigest();
        try self.logical.validate();
        if (!std.meta.eql(self.logical.command, try self.physical.logicalCommand())) return error.InvalidPrivateIndexNativeReceipt;
    }
};

/// Only the claims portion of physical validation. No schema/index-validation
/// ACK can be produced from this record without the independent generation proof.
pub const InventoryOutcome = struct {
    binding: jobs.Binding,
    record: private_claims.InventoryValidation.Record,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    pub fn validateFor(self: InventoryOutcome, request: BuildRequest) !void {
        try request.requireBinding(self.binding);
        const expected = request.target_expectation orelse return error.InvalidPrivateIndexNativeReceipt;
        try self.record.validate();
        if (!std.mem.eql(u8, &try expected.digest(), &try self.record.expected.digest()) or
            self.record.position != .native or !std.meta.eql(self.position, self.record.position.native) or !self.position.namespace.eql(request.identity.namespace) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
    }
};

pub const PreparedInventoryValidation = struct {
    pub const ApplyOutcome = InventoryOutcome;
    alloc: Allocator,
    batch: private_claims.InventoryValidation.Prepared,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,
    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.batch.context.ensureActive();
        try self.batch.command.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.batch.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .inventory_validation = self.batch.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) InventoryOutcome {
        return .{ .binding = self.batch.command.binding, .record = .{ .expected = self.batch.command.expected, .fields = self.batch.command.next, .position = .{ .native = position }, .page_digest = self.batch.command.digest() catch unreachable }, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), _: anytype, txn: anytype, replay: ?Native) !InventoryOutcome {
        const result = try self.batch.stage(self.alloc, txn, replay);
        return .{ .binding = self.batch.command.binding, .record = result.record, .position = result.record.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.batch.context.ensureActive();
        const binding = self.batch.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try self.batch.stageAt(self.alloc, txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return false;
    }
    pub fn populateReceipt(_: *const @This(), _: anytype, _: *InventoryOutcome) !void {}
    pub fn preparePayload(self: *@This()) !void {
        if (self.batch.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "inventory_validation", self.batch.command, private_claims.InventoryValidation.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Physical generation verification remains unsigned progress. Metadata must
/// still authenticate/join the physical proofs before accepting validation.
pub const GenerationOutcome = struct {
    binding: jobs.Binding,
    record: validation.Summary,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    pub fn validateFor(self: GenerationOutcome, request: BuildRequest) !void {
        try request.requireBinding(self.binding);
        const expected = request.target_expectation orelse return error.InvalidPrivateIndexNativeReceipt;
        try self.record.validate();
        if (!std.mem.eql(u8, &try expected.digest(), &try self.record.expected.digest()) or
            self.record.position != .native or !std.meta.eql(self.position, self.record.position.native) or !self.position.namespace.eql(request.identity.namespace) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
    }
};
pub const PreparedGenerationValidation = struct {
    pub const ApplyOutcome = GenerationOutcome;
    alloc: Allocator,
    page: validation.Prepared,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,
    pub fn prepare(alloc: Allocator, core: anytype, reservation: *const Prepared, expected: private_claims.TargetExpectation, budget: pages.Budget, context: RequestContext) !?@This() {
        if (reservation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        return prepareReservation(alloc, core, reservation, expected, budget, context);
    }
    pub fn prepareRaft(alloc: Allocator, core: anytype, reservation: *const @import("relational_index_raft_control.zig").Prepared, expected: private_claims.TargetExpectation, budget: pages.Budget, context: RequestContext) !?@This() {
        if (reservation.command.binding.authority != .raft) return error.InvalidControlReceiptPosition;
        return prepareReservation(alloc, core, reservation, expected, budget, context);
    }
    fn prepareReservation(alloc: Allocator, core: anytype, reservation: anytype, expected: private_claims.TargetExpectation, budget: pages.Budget, context: RequestContext) !?@This() {
        if (reservation.command.effect != .reserved) return error.PrivateIndexOwnerJobChanged;
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        var binding = reservation.command.binding;
        binding.phase = .reserved;
        const page = (try validation.Prepared.prepare(alloc, core, binding, expected, &artifacts.addition, budget, context)) orelse return null;
        return .{ .alloc = alloc, .page = page };
    }
    pub fn replay(alloc: Allocator, core: anytype, command: validation.Command) !@This() {
        try command.validate();
        var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
        defer view.release();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        try jobs.requireReserved(&read, command.binding);
        var manifest = try inputs.Prepared.load(alloc, &read, command.binding.inputs_digest);
        defer manifest.deinit();
        var artifacts = try Artifacts.init(alloc, view, &read, manifest.bytes, if (command.binding.authority == .native) .native else .raft);
        defer artifacts.deinit();
        if (!std.mem.eql(u8, &artifacts.unique.generation, &command.expected.generation)) return error.PrivateIndexOwnerJobChanged;
        return .{ .alloc = alloc, .page = try validation.Prepared.replay(alloc, core, &artifacts.addition, command) };
    }
    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.page.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.page.context.ensureActive();
        try self.page.command.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.page.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .generation_validation = self.page.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) GenerationOutcome {
        const command = self.page.command;
        return .{ .binding = command.binding, .record = .{ .expected = command.expected, .inventory_digest = command.inventory_digest, .source_members = command.source_members, .fields = command.next, .position = .{ .native = position }, .page_digest = command.digest() catch unreachable }, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), core: anytype, txn: anytype, position: ?Native) !GenerationOutcome {
        const result = try self.page.stage(core, txn, position);
        return .{ .binding = self.page.command.binding, .record = result.summary, .position = result.summary.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.page.context.ensureActive();
        const binding = self.page.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try self.page.stageAt(core, txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return false;
    }
    pub fn populateReceipt(_: *const @This(), _: anytype, _: *GenerationOutcome) !void {}
    pub fn preparePayload(self: *@This()) !void {
        if (self.page.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "generation_validation", self.page.command, validation.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

pub const ValidationSealOutcome = struct {
    command: validation.SealCommand,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,

    pub fn validate(self: @This()) !void {
        const expected = try self.command.receipt(self.position);
        if (self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
        if (self.receipt) |value| {
            if (self.replication_pending or self.error_name != null or !std.meta.eql(value, expected)) return error.InvalidPrivateIndexNativeReceipt;
        } else if (!self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
    }
};

pub const PreparedValidationSeal = struct {
    pub const ApplyOutcome = ValidationSealOutcome;
    alloc: Allocator,
    command: validation.SealCommand,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .validation_seal = self.command }, .position = position }).validate();
        _ = try self.command.receipt(position);
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) ValidationSealOutcome {
        return .{ .command = self.command, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), _: anytype, txn: anytype, position: ?Native) !ValidationSealOutcome {
        try self.validate();
        const result = try validation.Seal.stage(self.alloc, txn, self.command, position);
        return .{ .command = self.command, .position = result.seal.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        const binding = self.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try validation.Seal.stageAt(self.alloc, txn, self.command, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return true;
    }
    pub fn populateReceipt(self: *const @This(), txn: anytype, outcome: *ValidationSealOutcome) !void {
        const sealed = (try validation.Seal.load(self.alloc, txn, self.command.binding)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if (sealed.position != .native or !std.meta.eql(sealed.command, self.command) or !std.meta.eql(sealed.position.native, outcome.position)) return error.InvalidPrivateIndexNativeReceipt;
        outcome.receipt = try sealed.command.receipt(sealed.position.native);
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "validation_seal", self.command, validation.Seal.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Private receiver input. Metadata supplies the logical intent; filesystem
/// and physical-root identity come separately from the host's applying lease.
pub const InstallationRequest = struct {
    intent: validation.InstallIntent,
    budget: pages.Budget,

    pub fn validate(self: @This()) !void {
        try self.intent.validate();
        try self.budget.validate();
    }
};

/// Installation never exposes a logical receipt without its complete local
/// physical witness. Partial/failed scans are resumable observations only.
pub const InstallationResponse = union(enum) {
    audit: AuditOutcome,
    installed: struct { physical: AuditOutcome, logical: InstallOutcome },

    pub fn physicalAudit(self: @This()) AuditOutcome {
        return switch (self) {
            .audit => |value| value,
            .installed => |value| value.physical,
        };
    }

    pub fn validateFor(self: @This(), request: InstallationRequest) !void {
        try request.validate();
        const audited_physical = self.physicalAudit();
        if (!std.meta.eql(audited_physical.binding, request.intent.validation.command.binding) or
            !std.meta.eql(audited_physical.expected, request.intent.validation.command.expected)) return error.InvalidPrivateIndexNativeReceipt;
        try audited_physical.audit.validateFor(audited_physical.expected, audited_physical.audit.root_incarnation);
        if (self == .installed) {
            const value = self.installed;
            try value.logical.validate();
            if (!std.meta.eql(value.logical.command.intent(), request.intent) or
                !std.meta.eql(try audited_physical.logicalCommand(), request.intent.validation.command)) return error.InvalidPrivateIndexNativeReceipt;
        }
    }
};

pub const ReleaseOutcome = TerminalOutcome(release.Intent);
pub const CancellationReleaseOutcome = TerminalOutcome(release.CancellationIntent);
fn TerminalOutcome(comptime Intent: type) type {
    return struct {
        intent: Intent,
        position: Native,
        duplicate: bool,
        replication_pending: bool = false,
        error_name: ?[]const u8 = null,
        receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,
        pub fn validate(self: @This()) !void {
            const expected = try self.intent.receipt(self.position);
            if (self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
            if (self.receipt) |value| {
                if (self.replication_pending or self.error_name != null or !std.meta.eql(value, expected)) return error.InvalidPrivateIndexNativeReceipt;
            } else if (!self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
        }
    };
}

pub const CancellationCompletionOutcome = struct {
    binding: jobs.Binding,
    position: Native,
    duplicate: bool,
    record: ?jobs.Canceled = null,
    receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn validate(self: @This()) !void {
        _ = try self.binding.encode();
        try self.position.validate();
        if (self.binding.authority != .native or !self.position.namespace.eql(self.binding.fence.namespace) or (self.replication_pending and self.error_name == null)) return error.InvalidPrivateIndexCancellation;
        if (self.record) |record| {
            try record.validate();
            if (record.position != .native or !std.meta.eql(record.binding, self.binding) or !std.meta.eql(record.position.native, self.position)) return error.InvalidPrivateIndexCancellation;
        }
        if (self.receipt) |receipt| {
            const record = self.record orelse return error.InvalidPrivateIndexCancellation;
            if (self.replication_pending or self.error_name != null or !std.meta.eql(receipt, try record.receipt())) return error.InvalidPrivateIndexCancellation;
        } else if (!self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexCancellation;
    }
};

/// Cleanup completion uses the common native commit/outbox boundary. The
/// trusted receiving adapter must independently obtain metadata permission.
pub const PreparedCancellationCompletion = struct {
    pub const ApplyOutcome = CancellationCompletionOutcome;
    alloc: Allocator,
    binding: jobs.Binding,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        _ = try self.binding.encode();
        if (self.binding.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .cancel_complete = self.binding }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) ApplyOutcome {
        return .{ .binding = self.binding, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), core: anytype, txn: anytype, replay: ?Native) !ApplyOutcome {
        try self.validate();
        var manager = try core.initTxnManager();
        defer manager.deinit();
        const result = try jobs.Canceled.stage(txn, &manager, self.binding, replay);
        return .{ .binding = self.binding, .position = result.record.position.native, .record = result.record, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        if (self.binding.authority != .raft or owner_group == 0 or owner_group != self.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        var manager = try core.initTxnManager();
        defer manager.deinit();
        _ = try jobs.Canceled.stageAt(txn, &manager, self.binding, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return true;
    }
    pub fn populateReceipt(self: *const @This(), txn: anytype, outcome: *ApplyOutcome) !void {
        const record = (try jobs.Canceled.load(txn)) orelse return error.InvalidPrivateIndexCancellation;
        if (record.position != .native or !std.meta.eql(record.binding, self.binding) or !std.meta.eql(record.position.native, outcome.position)) return error.PrivateIndexOwnerJobChanged;
        try record.requireCurrent(txn);
        outcome.record = record;
        outcome.receipt = try record.receipt();
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "cancel_complete", self.binding, max_cancellation_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

pub const PreparedRelease = struct {
    pub const ApplyOutcome = ReleaseOutcome;
    alloc: Allocator,
    intent: release.Intent,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, core: *@import("core.zig").DBCore, intent: release.Intent, context: RequestContext) !@This() {
        try context.ensureActive();
        try intent.validate();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        if (try release.Record.load(alloc, &read, intent) != null) try release.Record.verifyRecovery(alloc, &read) else try intent.requireInstalled(alloc, &read);
        return .{ .alloc = alloc, .intent = intent, .context = context };
    }
    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.intent.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.intent.installed.command.validation.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .release = self.intent }, .position = position }).validate();
        _ = try self.intent.receipt(position);
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) ReleaseOutcome {
        return .{ .intent = self.intent, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), _: anytype, txn: anytype, position: ?Native) !ReleaseOutcome {
        try self.validate();
        const result = try release.Record.stage(self.alloc, txn, self.intent, position);
        return .{ .intent = self.intent, .position = result.record.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        const binding = self.intent.installed.command.validation.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try release.Record.stageAt(self.alloc, txn, self.intent, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return true;
    }
    pub fn populateReceipt(self: *const @This(), txn: anytype, outcome: *ReleaseOutcome) !void {
        const record = (try release.Record.load(self.alloc, txn, self.intent)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if (record.position != .native or !std.meta.eql(record.position.native, outcome.position)) return error.InvalidPrivateIndexNativeReceipt;
        try release.Record.verifyRecovery(self.alloc, txn);
        outcome.receipt = try self.intent.receipt(record.position.native);
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.intent.installed.position != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "release", self.intent, release.Record.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// The accepted cleanup cut enters the same commit/outbox/replay machinery as
/// published release; the host must independently authorize its root/barrier.
pub const PreparedCancellationRelease = struct {
    pub const ApplyOutcome = CancellationReleaseOutcome;
    alloc: Allocator,
    intent: release.CancellationIntent,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, core: *@import("core.zig").DBCore, intent: release.CancellationIntent, context: RequestContext) !@This() {
        try context.ensureActive();
        try intent.validate();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        if (try release.CancellationRecord.load(&read, intent) != null) try release.CancellationRecord.verifyRecovery(&read) else try intent.requireCurrent(&read);
        return .{ .alloc = alloc, .intent = intent, .context = context };
    }
    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.intent.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.intent.canceled.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .cancel_release = self.intent }, .position = position }).validate();
        _ = try self.intent.receipt(position);
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) ApplyOutcome {
        return .{ .intent = self.intent, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), core: anytype, txn: anytype, replay: ?Native) !ApplyOutcome {
        try self.validate();
        var manager = try core.initTxnManager();
        defer manager.deinit();
        const result = try release.CancellationRecord.stage(txn, &manager, self.intent, replay);
        return .{ .intent = self.intent, .position = result.record.position.native, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *@This(), core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        const binding = self.intent.canceled.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        var manager = try core.initTxnManager();
        defer manager.deinit();
        _ = try release.CancellationRecord.stageAt(txn, &manager, self.intent, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return true;
    }
    pub fn populateReceipt(self: *const @This(), txn: anytype, outcome: *ApplyOutcome) !void {
        const record = (try release.CancellationRecord.load(txn, self.intent)) orelse return error.InvalidPrivateIndexNativeReceipt;
        if (record.position != .native or !std.meta.eql(record.position.native, outcome.position)) return error.InvalidPrivateIndexNativeReceipt;
        try release.CancellationRecord.verifyRecovery(txn);
        outcome.receipt = try self.intent.receipt(record.position.native);
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.intent.canceled.position != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "cancel_release", self.intent, max_cancellation_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// The page or decoded replay frame owns command strings through apply.
pub const PreparedReleaseGc = struct {
    pub const ApplyOutcome = release.Gc.Outcome;
    alloc: Allocator,
    command: release.Gc.Command,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.command.record.intent.installed.command.validation.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .release_gc = self.command }, .position = position }).validate();
        if (position.sequence <= self.command.record.position.native.sequence) return error.InvalidPrivateIndexNativeReceipt;
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) ApplyOutcome {
        return .{ .released = self.command.record.position.native, .position = position, .duplicate = true };
    }
    pub fn stage(self: *@This(), _: anytype, txn: anytype, position: ?Native) !ApplyOutcome {
        try self.validate();
        return self.command.stage(self.alloc, txn, position);
    }
    pub fn stageRaft(self: *@This(), _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        const binding = self.command.record.intent.installed.command.validation.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try self.command.stageAt(self.alloc, txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return false;
    }
    pub fn populateReceipt(_: *const @This(), _: anytype, _: *ApplyOutcome) !void {}
    pub fn preparePayload(self: *@This()) !void {
        if (self.command.record.position != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "release_gc", self.command, release.Gc.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

pub const InstallOutcome = struct {
    command: validation.InstallCommand,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    receipt: ?@import("relational_index_publication_progress.zig").Receipt = null,
    pub fn validate(self: @This()) !void {
        const expected = try self.command.receipt(self.position);
        if (self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
        if (self.receipt) |value| {
            if (self.replication_pending or self.error_name != null or !std.meta.eql(value, expected)) return error.InvalidPrivateIndexNativeReceipt;
        } else if (!self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
    }
};

/// Owned off-lock installation preparation. Independent metadata/root
/// authorization belongs to the receiving owner, not to this logical command.
pub const PreparedInstall = struct {
    pub const ApplyOutcome = InstallOutcome;
    const SchemaData = struct {
        prepared: @import("core.zig").PreparedSchemaMetadata,
        json: []u8,
        version_key: []u8,
    };
    alloc: Allocator,
    command: validation.InstallCommand,
    context: RequestContext = .{},
    schema_data: ?SchemaData = null,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, core: *@import("core.zig").DBCore, command: validation.InstallCommand, context: RequestContext) !@This() {
        try command.validate();
        return initMode(alloc, core, command.intent(), command, context);
    }
    /// Receivers pass metadata's logical decision/validation identity only.
    /// Physical schema and activation hashes are derived locally, never chosen
    /// by the RPC caller. This does not replace current-root authorization.
    pub fn initIntent(alloc: Allocator, core: *@import("core.zig").DBCore, intent: validation.InstallIntent, context: RequestContext) !@This() {
        return initMode(alloc, core, intent, null, context);
    }
    fn initMode(alloc: Allocator, core: *@import("core.zig").DBCore, intent: validation.InstallIntent, expected: ?validation.InstallCommand, context: RequestContext) !@This() {
        try context.ensureActive();
        try intent.validate();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const binding = intent.validation.command.binding;
        if (try validation.Installed.load(alloc, &read, binding)) |installed| {
            if (!std.meta.eql(installed.command.intent(), intent)) return error.InvalidPrivateIndexNativeReceipt;
            if (expected) |command| if (!std.meta.eql(installed.command, command)) return error.InvalidPrivateIndexNativeReceipt;
            try installed.requireCurrent(alloc, &read);
            return .{ .alloc = alloc, .command = installed.command, .context = context };
        }
        try intent.validateBefore(alloc, &read);
        var candidate = try jobs.reopenCandidate(alloc, &read, binding);
        defer candidate.deinit();
        const view = try inputs.decode(candidate.inputs.bytes);
        const json = try alloc.dupe(u8, view.after_json);
        errdefer alloc.free(json);
        const version_key = try schema_api.versionedSchemaKeyAlloc(alloc, binding.physical_candidate.schema_version);
        errdefer alloc.free(version_key);
        var parsed = try schema_api.parseValidatedTableSchema(alloc, json);
        defer parsed.deinit(alloc);
        const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
        defer @import("../schema.zig").freeSchema(alloc, runtime);
        const metadata = [_]@import("../docstore.zig").KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = json }, .{ .key = version_key, .value = json } };
        var prepared = try core.prepareSchemaMetadataReservedIndex(runtime, &metadata, .{ .binding = binding, .candidate = &candidate });
        errdefer prepared.deinit();
        const activation = prepared.reserved_activation orelse return error.PrivateIndexTargetValidationRequired;
        const command: validation.InstallCommand = .{
            .validation = intent.validation,
            .metadata_decision = intent.metadata_decision,
            .schema_digest = inputs.digest(prepared.encoded),
            .activation_digest = inputs.digest(activation.value),
        };
        try command.validate();
        if (expected) |supplied| if (!std.meta.eql(command, supplied)) return error.InvalidPrivateIndexNativeReceipt;
        try context.ensureActive();
        return .{ .alloc = alloc, .command = command, .context = context, .schema_data = .{ .prepared = prepared, .json = json, .version_key = version_key } };
    }
    pub fn deinit(self: *@This()) void {
        if (self.schema_data) |*data| {
            data.prepared.deinit();
            self.alloc.free(data.version_key);
            self.alloc.free(data.json);
        }
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.command.validation.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try (Frame{ .effect = .{ .install = self.command }, .position = position }).validate();
        _ = try self.command.receipt(position);
    }
    pub fn replayDuplicateOutcome(self: *const @This(), position: Native) InstallOutcome {
        return .{ .command = self.command, .position = position, .duplicate = true };
    }
    pub fn requiresSchemaCommit(self: *const @This()) bool {
        return if (self.schema_data) |data| data.prepared.publication != null else false;
    }
    pub fn commitSchema(self: *@This(), core: *@import("core.zig").DBCore, position: ?Native, participant: anytype) !InstallOutcome {
        if (self.command.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        const installed = try self.commitSchemaAt(core, if (position) |cut| .{ .native = cut } else null, participant);
        return .{ .command = self.command, .position = installed.position.native, .duplicate = false };
    }
    pub fn commitSchemaAt(self: *@This(), core: *@import("core.zig").DBCore, position: ?@import("receipt_position.zig").Position, participant: anytype) !validation.Installed {
        try self.validate();
        const data = if (self.schema_data) |*value| value else return error.PrivateIndexPublicationRequired;
        const metadata = [_]@import("../docstore.zig").KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = data.json }, .{ .key = data.version_key, .value = data.json } };
        return core.commitPreparedSchemaMetadataReservedIndexAt(&data.prepared, &metadata, self.command, position, participant);
    }
    /// Only exact installed retries use the ordinary transaction branch. New
    /// installation must use the shared schema writer above, never a second
    /// independent schema/catalog transaction.
    pub fn stage(self: *@This(), _: anytype, txn: anytype, position: ?Native) !InstallOutcome {
        if (self.command.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try self.validate();
        if (self.requiresSchemaCommit()) return error.PrivateIndexPublicationRequired;
        const installed = (try validation.Installed.load(self.alloc, txn, self.command.validation.command.binding)) orelse return error.InvalidPrivateIndexNativeReceipt;
        try installed.requireCurrent(self.alloc, txn);
        if (!std.meta.eql(installed.command, self.command)) return error.InvalidPrivateIndexNativeReceipt;
        if (position) |expected| if (!std.meta.eql(expected, installed.position.native)) return error.InvalidPrivateIndexNativeReceipt;
        return self.replayDuplicateOutcome(installed.position.native);
    }
    pub fn stageRaft(self: *@This(), _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        const binding = self.command.validation.command.binding;
        if (binding.authority != .raft or owner_group == 0 or owner_group != binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        if (self.requiresSchemaCommit()) return error.PrivateIndexPublicationRequired;
        const installed = (try validation.Installed.load(self.alloc, txn, binding)) orelse return error.InvalidControlReceiptPosition;
        try installed.requireCurrent(self.alloc, txn);
        if (!std.meta.eql(installed.command, self.command)) return error.InvalidControlReceiptPosition;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        if (installed.position != .raft or entry.index <= installed.position.raft.index or entry.term < installed.position.raft.term) return error.InvalidControlReceiptPosition;
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return true;
    }
    pub fn populateReceipt(self: *const @This(), txn: anytype, outcome: *InstallOutcome) !void {
        const installed = (try validation.Installed.load(self.alloc, txn, self.command.validation.command.binding)) orelse return error.InvalidPrivateIndexNativeReceipt;
        try installed.requireCurrent(self.alloc, txn);
        if (installed.position != .native or !std.meta.eql(installed.command, self.command) or !std.meta.eql(installed.position.native, outcome.position)) return error.InvalidPrivateIndexNativeReceipt;
        outcome.receipt = try self.command.receipt(installed.position.native);
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.command.validation.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "install", self.command, validation.Installed.max_wire_bytes);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

pub const PreparedBuild = struct {
    pub const ApplyOutcome = BuildOutcome;
    pub const checkpoint_key = jobs.key ++ ":build-page";
    pub const checkpoint_len = 208;
    alloc: Allocator,
    page: pages.Page,
    command: pages.Page.PrivateCommand,
    command_digest: [32]u8,
    progress_digest: [32]u8,
    claim_batch: private_claims.SourceStream.PreparedSource,
    /// A cloned plan still uses its compilation allocator. Keep that allocator
    /// pinned through page destruction even if the resident cache is evicted.
    cache_lease: ?BuildCache.Lease = null,
    context: RequestContext = .{},
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    /// Takes page ownership only on success.
    pub fn init(alloc: Allocator, page: pages.Page) !PreparedBuild {
        const command = try page.privateCommand();
        const command_digest = try command.digest();
        const progress_digest = try command.encodedProgressDigest();
        var rows: [private_claims.max_claims]private_claims.SourceStream.Row = undefined;
        var count: usize = 0;
        for (command.candidates) |candidate| if (candidate.unique_tuple) |tuple| {
            rows[count] = .{ .key = candidate.document, .tuple = tuple };
            count += 1;
        };
        const claim_batch = try private_claims.SourceStream.PreparedSource.init(alloc, command.binding, command.source, progress_digest, switch (command.next.state) {
            .building => .building,
            .ready => .ready,
            .failed => .failed,
        }, rows[0..count]);
        return .{ .alloc = alloc, .page = page, .command = command, .command_digest = command_digest, .progress_digest = progress_digest, .claim_batch = claim_batch };
    }
    /// Reuse the compiled reservation across pages. Only the page's source
    /// snapshot and scratch arena are renewed; no per-page schema compilation.
    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, table_name: []const u8, reservation: *const Prepared, budget: pages.Budget) !?PreparedBuild {
        return prepareControlled(alloc, io, core, table_name, reservation, budget, .{});
    }
    pub fn prepareControlled(alloc: Allocator, io: ?std.Io, core: anytype, table_name: []const u8, reservation: *const Prepared, budget: pages.Budget, context: RequestContext) !?PreparedBuild {
        try context.ensureActive();
        try reservation.command.validate();
        if (reservation.command.effect != .reserved) return error.PrivateIndexOwnerJobChanged;
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        return prepareCandidate(alloc, io, core, table_name, reservation.command.binding, artifacts, budget, context);
    }
    pub fn prepareRaft(alloc: Allocator, io: ?std.Io, core: anytype, table_name: []const u8, reservation: *const @import("relational_index_raft_control.zig").Prepared, budget: pages.Budget, request_context: RequestContext) !?PreparedBuild {
        try request_context.ensureActive();
        try reservation.command.validate();
        if (reservation.command.effect != .reserved or reservation.command.binding.authority != .raft) return error.PrivateIndexOwnerJobChanged;
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        return prepareCandidate(alloc, io, core, table_name, reservation.command.binding, artifacts, budget, request_context);
    }
    fn prepareCandidate(alloc: Allocator, io: ?std.Io, core: anytype, table_name: []const u8, original: jobs.Binding, artifacts: *const Artifacts, budget: pages.Budget, request_context: RequestContext) !?PreparedBuild {
        var binding = original;
        binding.phase = .reserved;
        var page = (try pages.Page.preparePrivateClaims(alloc, io, core, binding, &artifacts.addition, budget, request_context, .{ .scope = .{ .table_name = table_name, .generation = artifacts.unique.generation, .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = artifacts.unique.tuple.fingerprint, .nulls_not_distinct = artifacts.unique.nulls_not_distinct }, .tuple = artifacts.unique.tuple })) orelse return null;
        errdefer page.deinit();
        var result = try init(alloc, page);
        result.context = request_context;
        return result;
    }
    pub fn replayPrepared(alloc: Allocator, core: anytype, command: pages.Page.PrivateCommand) !PreparedBuild {
        try command.validate();
        var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
        defer view.release();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        try jobs.requireReserved(&read, command.binding);
        var manifest = try inputs.Prepared.load(alloc, &read, command.binding.inputs_digest);
        defer manifest.deinit();
        var artifacts = try Artifacts.init(alloc, view, &read, manifest.bytes, if (command.binding.authority == .native) .native else .raft);
        defer artifacts.deinit();
        if (!std.mem.eql(u8, &artifacts.unique.generation, &command.source.generation) or
            !std.mem.eql(u8, &artifacts.unique.tuple.fingerprint, &command.source.tuple_fingerprint) or
            artifacts.unique.nulls_not_distinct != command.source.nulls_not_distinct) return error.InvalidPrivateIndexClaimInventory;
        var page = try pages.Page.replayPrivate(alloc, core, &artifacts.addition, command);
        errdefer page.deinit();
        return try init(alloc, page);
    }
    pub fn deinit(self: *PreparedBuild) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.claim_batch.deinit();
        self.page.deinit();
        if (self.cache_lease) |*lease| lease.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedBuild) !void {
        try self.context.ensureActive();
        try self.command.validate();
    }
    pub fn namespace(self: *const PreparedBuild) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedBuild, position: Native) !void {
        try (Frame{ .effect = .{ .build = self.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedBuild, position: Native) BuildOutcome {
        return .{ .binding = self.command.binding, .page_digest = self.command_digest, .position = position, .duplicate = true };
    }
    fn hash(bytes: []const u8) [32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &result, .{});
        return result;
    }
    fn checkpoint(txn: anytype, binding: jobs.Binding) !?[checkpoint_len]u8 {
        const raw = txn.get(checkpoint_key) catch |err| {
            if (err == error.NotFound) return null;
            return err;
        };
        if (raw.len != checkpoint_len or !std.mem.eql(u8, raw[0..8], "AIBP\x02\x00\x00\x00") or
            !std.mem.allEqual(u8, raw[137..144], 0) or !std.mem.eql(u8, raw[176..208], &hash(raw[0..176]))) return error.InvalidRelationalIndexProgress;
        // Copy before another store lookup can invalidate the borrowed value.
        const bytes = raw[0..checkpoint_len].*;
        if (!std.mem.eql(u8, bytes[8..40], &hash(&try binding.encode()))) return null;
        const position = try @import("receipt_position.zig").Position.decode(bytes[104..137]);
        try position.requireNamespace(binding.fence.namespace);
        if ((position == .native) != (binding.authority == .native)) return error.InvalidControlReceiptPosition;
        const state = try source_authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        switch (position) {
            .native => |value| if (value.sequence > state.sequence) return error.InvalidControlReceiptPosition,
            .raft => |value| {
                const applied = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
                if (value.index > applied.index or value.term > applied.term or (value.index == applied.index and value.term != applied.term)) return error.InvalidControlReceiptPosition;
            },
        }
        const progress = try txn.get(&pages.progressKey(binding.index));
        if (!std.mem.eql(u8, bytes[72..104], &hash(progress))) return error.InvalidRelationalIndexProgress;
        return bytes;
    }
    pub fn verifyRecovery(txn: anytype, binding: jobs.Binding) !void {
        const bytes = try checkpoint(txn, binding);
        try private_claims.SourceStream.verifyRecoveryOrdered(txn, binding, if (bytes) |value| try @import("receipt_position.zig").Position.decode(value[104..137]) else null, if (bytes) |value| value[72..104].* else null, if (bytes) |value| value[144..176].* else null);
    }
    /// Pin the physical checkpoint and the complete bounded header chain in
    /// the same snapshot before exposing source bytes to a delivery worker.
    /// The owned batch is data, not a target acknowledgment or release proof.
    pub fn peekClaimSource(alloc: Allocator, txn: anytype, binding: jobs.Binding) !?private_claims.SourceStream.OwnedBatch {
        try verifyRecovery(txn, binding);
        return private_claims.SourceStream.peek(alloc, txn, binding);
    }
    pub fn completion(txn: anytype, table_name: []const u8, reservation: *const Prepared) !BuildCompletion {
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        var binding = reservation.command.binding;
        binding.phase = .reserved;
        try jobs.requireReserved(txn, binding);
        const bytes = (try checkpoint(txn, binding)) orelse return error.InvalidRelationalIndexProgress;
        const index = for (artifacts.addition.plan.boundIndexes()) |value| {
            if (value.id().mapKey() == binding.index.mapKey()) break value;
        } else return error.PrivateIndexOwnerJobChanged;
        const owner = try pages.ownership(txn);
        const progress = try pages.Progress.decode(try txn.get(&pages.progressKey(binding.index)));
        if (!progress.matches(index, owner) or progress.phase != .primary or progress.state == .building)
            return error.InvalidRelationalIndexProgress;
        const position = (try @import("receipt_position.zig").Position.decode(bytes[104..137])).native;
        const source = try private_claims.SourceStream.requireLatest(txn, binding, .{ .table_name = table_name, .generation = artifacts.unique.generation, .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = artifacts.unique.tuple.fingerprint, .nulls_not_distinct = artifacts.unique.nulls_not_distinct }, position, bytes[72..104].*, bytes[144..176].*);
        return .{ .binding = binding, .state = if (progress.state == .ready) .ready else .failed, .failure = progress.failure, .rows_scanned = progress.rows_scanned, .progress_digest = bytes[72..104].*, .position = position, .claim_batches_pending = source.pending(), .claim_bytes_pending = source.pending_bytes };
    }
    /// A bounded coherent observation; the retained page cut, not the current
    /// host term or watermark, identifies the physical work that committed.
    pub fn raftProgress(txn: anytype, table_name: []const u8, reservation: *const @import("relational_index_raft_control.zig").Prepared) !@import("relational_index_raft_contract.zig").BuildProgress {
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        var binding = reservation.command.binding;
        binding.phase = .reserved;
        if (binding.authority != .raft) return error.InvalidControlReceiptPosition;
        try jobs.requireReserved(txn, binding);
        try verifyRecovery(txn, binding);
        const bytes = (try checkpoint(txn, binding)) orelse return .{ .binding = binding };
        const index = for (artifacts.addition.plan.boundIndexes()) |value| {
            if (value.id().mapKey() == binding.index.mapKey()) break value;
        } else return error.PrivateIndexOwnerJobChanged;
        const owner = try pages.ownership(txn);
        const progress = try pages.Progress.decode(try txn.get(&pages.progressKey(binding.index)));
        if (!progress.matches(index, owner) or progress.phase != .primary) return error.InvalidRelationalIndexProgress;
        const position = try @import("receipt_position.zig").Position.decode(bytes[104..137]);
        const source = try private_claims.SourceStream.requireLatestOrdered(txn, binding, .{ .table_name = table_name, .generation = artifacts.unique.generation, .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = artifacts.unique.tuple.fingerprint, .nulls_not_distinct = artifacts.unique.nulls_not_distinct }, position, bytes[72..104].*, bytes[144..176].*);
        return .{ .binding = binding, .state = progress.state, .failure = progress.failure, .rows_scanned = progress.rows_scanned, .progress_digest = bytes[72..104].*, .position = position, .claim_batches_pending = source.pending(), .claim_bytes_pending = source.pending_bytes };
    }
    pub fn stage(self: *PreparedBuild, core: anytype, txn: anytype, replay: ?Native) !BuildOutcome {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        const outcome = try self.stageOrdered(core, txn, if (replay) |value| .{ .native = value } else null);
        return .{ .binding = self.command.binding, .page_digest = self.command_digest, .position = outcome.position.native, .duplicate = outcome.duplicate };
    }
    /// Trusted committed-entry execution only. No request supplies a cut and
    /// no native sequence is advanced for a replicated physical page.
    pub fn stageRaft(self: *PreparedBuild, core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        if (self.command.binding.authority != .raft or owner_group == 0 or owner_group != self.command.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try self.stageOrdered(core, txn, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    fn stageOrdered(self: *PreparedBuild, core: anytype, txn: anytype, replay: ?@import("receipt_position.zig").Position) !struct { position: @import("receipt_position.zig").Position, duplicate: bool } {
        try self.context.ensureActive();
        const binding = self.command.binding;
        try jobs.requireReserved(txn, binding);
        if (try checkpoint(txn, binding)) |bytes| {
            if (std.mem.eql(u8, bytes[40..72], &self.command_digest)) {
                if (!std.mem.eql(u8, bytes[72..104], &self.progress_digest)) return error.InvalidRelationalIndexProgress;
                const position = try @import("receipt_position.zig").Position.decode(bytes[104..137]);
                _ = try private_claims.SourceStream.requireLatestOrdered(txn, binding, self.command.source, position, self.progress_digest, bytes[144..176].*);
                if (replay) |expected| if (binding.authority == .native and !std.meta.eql(position, expected)) return error.InvalidControlReceiptPosition;
                return .{ .position = position, .duplicate = true };
            }
        }
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try self.page.stage(core, &manager, txn);
        const position: @import("receipt_position.zig").Position = if (binding.authority == .raft) replay orelse return error.InvalidControlReceiptPosition else .{ .native = .{ .namespace = self.namespace(), .sequence = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.namespace()), if (replay) |value| value.native.sequence else null) } };
        try self.claim_batch.stageOrdered(txn, binding, position);
        var bytes: [checkpoint_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AIBP\x02\x00\x00\x00");
        @memcpy(bytes[8..40], &hash(&try binding.encode()));
        @memcpy(bytes[40..72], &self.command_digest);
        @memcpy(bytes[72..104], &self.progress_digest);
        @memcpy(bytes[104..137], &try position.encode());
        @memcpy(bytes[144..176], self.claim_batch.header[288..320]);
        @memcpy(bytes[176..208], &hash(bytes[0..176]));
        try txn.put(checkpoint_key, &bytes);
        return .{ .position = position, .duplicate = false };
    }
    pub fn hasPublicationReceipt(_: *const PreparedBuild) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedBuild, _: anytype, _: *BuildOutcome) !void {}
    pub fn preparePayload(self: *PreparedBuild) !void {
        if (self.command.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "build", self.command, pages.Page.PrivateCommand.max_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedBuild, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// One resource-accounted immutable candidate per resident owner. Single-flight
/// compilation runs outside apply exclusion; leases pin the allocator through
/// page deinit, including concurrent eviction. The cache grants no authority.
pub const BuildCache = struct {
    mutex: std.Io.Mutex = .init,
    io: ?std.Io = null,
    manager: ?*resources.ResourceManager = null,
    reclaimer: u64 = 0,
    entry: ?*Entry = null,
    compiling: bool = false,
    epoch: u64 = 0,
    waiters: Waiters = .{},
    compilations: u64 = 0,
    hits: u64 = 0,

    const Entry = struct {
        backing: Allocator,
        header: ?resources.Reservation,
        budget: ?resources.BudgetedAllocator,
        refs: std.atomic.Value(usize) = .init(1),
        digest: [32]u8,
        prepared: ?union(enum) { native: Prepared, raft: @import("relational_index_raft_control.zig").Prepared } = null,
        history: ?validation.Installed.History = null,

        fn create(backing: Allocator, manager: ?*resources.ResourceManager, binding_digest: [32]u8) !*Entry {
            var header = if (manager) |value| try value.reserve(.relational_preparation_working_set, @sizeOf(Entry)) else null;
            errdefer if (header) |*reservation| reservation.release();
            const entry = try backing.create(Entry);
            entry.* = .{ .backing = backing, .header = header, .budget = if (manager) |value| resources.BudgetedAllocator.initReclaiming(value, .relational_preparation_working_set, backing, 1) else null, .digest = binding_digest };
            return entry;
        }
        fn retain(self: *Entry) void {
            const old = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(old > 0 and old < std.math.maxInt(usize));
        }
        fn release(self: *Entry) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.prepared) |*value| switch (value.*) {
                inline else => |*prepared| prepared.deinit(),
            };
            if (self.history) |*value| value.deinit();
            if (self.budget) |*value| value.deinit();
            const backing = self.backing;
            var header = self.header;
            backing.destroy(self);
            if (header) |*value| value.release();
        }
        fn bytes(self: *const Entry) u64 {
            return (if (self.header) |value| value.bytes else @as(u64, 0)) +
                (if (self.budget) |value| value.reservation.bytes else @as(u64, 0));
        }
    };

    pub const Lease = struct {
        entry: *Entry,
        pub fn reservation(self: Lease) *const Prepared {
            return &self.entry.prepared.?.native;
        }
        pub fn raftReservation(self: Lease) *const @import("relational_index_raft_control.zig").Prepared {
            return &self.entry.prepared.?.raft;
        }
        pub fn installedHistory(self: Lease) ?*const validation.Installed.History {
            return if (self.entry.history) |*value| value else null;
        }
        pub fn deinit(self: *Lease) void {
            self.entry.release();
            self.* = undefined;
        }
    };

    pub fn evict(self: *BuildCache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        const entry = self.entry;
        self.entry = null;
        self.epoch +%= 1;
        self.mutex.unlock(io);
        if (entry) |value| value.release();
    }
    pub fn deinit(self: *BuildCache, io: std.Io) void {
        // Resident-owner leases drain before DB close, just like schema views.
        std.debug.assert(!self.compiling and self.waiters.head == null);
        if (self.manager) |value| value.unregisterReclaimer(self.reclaimer);
        self.evict(io);
        self.reclaimer = 0;
        self.manager = null;
        self.io = null;
    }
    fn reclaim(ptr: *anyopaque, _: u64) u64 {
        const self: *BuildCache = @ptrCast(@alignCast(ptr));
        if (!self.mutex.tryLock()) return 0;
        const entry = self.entry orelse {
            self.mutex.unlock(self.io.?);
            return 0;
        };
        if (entry.refs.load(.acquire) != 1) {
            self.mutex.unlock(self.io.?);
            return 0;
        }
        const bytes = entry.bytes();
        self.entry = null;
        self.mutex.unlock(self.io.?);
        entry.release();
        return bytes;
    }
    fn finish(self: *BuildCache) void {
        self.compiling = false;
        while (self.waiters.pop()) |waiter| waiter.handoff.publish();
    }
    fn wait(self: *BuildCache, io: std.Io, waiter: *Waiters.Waiter, context: RequestContext) !void {
        defer {
            self.mutex.lockUncancelable(io);
            _ = self.waiters.remove(waiter);
            self.mutex.unlock(io);
        }
        while (!waiter.handoff.isAdmitted()) {
            try context.ensureActive();
            waiter.handoff.ready.waitTimeout(io, .{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(5), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return err,
            };
        }
        try context.ensureActive();
    }

    /// Caller observes the actual durable reserved binding before acquisition;
    /// page preparation and commit must independently recheck it afterwards.
    pub fn acquire(self: *BuildCache, io: std.Io, backing: Allocator, manager: ?*resources.ResourceManager, core: anytype, request: BuildRequest, binding: jobs.Binding, context: RequestContext) !Lease {
        return self.acquireMode(io, backing, manager, core, request, binding, context, request.binding_digest, false, .native);
    }

    pub fn acquireRaft(self: *BuildCache, io: std.Io, backing: Allocator, manager: ?*resources.ResourceManager, core: anytype, request: BuildRequest, binding: jobs.Binding, context: RequestContext) !Lease {
        return self.acquireMode(io, backing, manager, core, request, binding, context, request.binding_digest, false, .raft);
    }

    /// Audit-only acquisition: current installed state is checked even for
    /// warm hits. The cached predecessor view grants no execution permission.
    pub fn acquireInstalledAudit(self: *BuildCache, io: std.Io, backing: Allocator, manager: ?*resources.ResourceManager, core: anytype, request: BuildRequest, intent: validation.InstallIntent, context: RequestContext) !Lease {
        try context.ensureActive();
        try intent.validate();
        const cache_digest = digest: {
            var read = try core.store.beginReadTxn();
            defer read.abort();
            const record = (try validation.Installed.load(backing, &read, intent.validation.command.binding)) orelse return error.InvalidPrivateIndexNativeReceipt;
            try record.requireCurrent(backing, &read);
            if (!std.meta.eql(record.command.intent(), intent)) return error.InvalidPrivateIndexNativeReceipt;
            break :digest try record.command.evidenceDigest();
        };
        return self.acquireMode(io, backing, manager, core, request, intent.validation.command.binding, context, cache_digest, true, .native);
    }

    fn acquireMode(self: *BuildCache, io: std.Io, backing: Allocator, manager: ?*resources.ResourceManager, core: anytype, request: BuildRequest, binding: jobs.Binding, context: RequestContext, cache_digest: [32]u8, comptime installed_audit: bool, comptime authority: @import("relational_index_publication_progress.zig").Authority) !Lease {
        try context.ensureActive();
        if (comptime authority == .native) try request.requireBinding(binding) else try request.requireRaftBinding(binding);
        const epoch = while (true) {
            try self.mutex.lock(io);
            var locked = true;
            errdefer if (locked) self.mutex.unlock(io);
            try context.ensureActive();
            if (self.io != null and self.manager != manager) return error.PrivateIndexOwnerJobChanged;
            self.io = io;
            self.manager = manager;
            if (manager) |value| if (self.reclaimer == 0) {
                self.reclaimer = try value.registerReclaimer(.relational_preparation_working_set, self, reclaim);
            };
            if (self.entry) |entry| if ((entry.history != null) == installed_audit and std.mem.eql(u8, &entry.digest, &cache_digest)) {
                entry.retain();
                self.hits +|= 1;
                self.mutex.unlock(io);
                return .{ .entry = entry };
            };
            if (self.compiling) {
                var waiter: Waiters.Waiter = .{ .handoff = .{ .io = io }, .payload = {} };
                self.waiters.enqueue(&waiter);
                self.mutex.unlock(io);
                locked = false;
                try self.wait(io, &waiter, context);
                continue;
            }
            self.compiling = true;
            const generation = self.epoch;
            const old = self.entry;
            self.entry = null;
            self.mutex.unlock(io);
            if (old) |value| value.release();
            break generation;
        };
        errdefer {
            self.mutex.lockUncancelable(io);
            self.finish();
            self.mutex.unlock(io);
        }
        const entry = try Entry.create(backing, manager, cache_digest);
        errdefer entry.release();
        const alloc = if (entry.budget) |*value| value.allocator() else backing;
        if (comptime installed_audit) {
            var read = try core.store.beginReadTxn();
            defer read.abort();
            const current = (try jobs.load(&read)) orelse return error.PrivateIndexJobRequired;
            entry.history = validation.Installed.History.init(alloc, &read, current) catch |err| {
                if (entry.budget) |value| if (value.denied()) return error.ResourceBudgetExceeded;
                return err;
            };
            const history = &entry.history.?;
            if (!std.meta.eql(history.binding, binding) or !std.mem.eql(u8, &try history.record.command.evidenceDigest(), &cache_digest)) return error.PrivateIndexOwnerJobChanged;
            var historical = history.reader(&read);
            var view = (try core.acquireSchemaVersionView(binding.physical_reserved.schema_version)) orelse return error.RelationalTableRequired;
            defer view.release();
            var command: Command = .{ .effect = .reserved, .binding = binding };
            command.binding.phase = .fencing;
            const prepared = Prepared.control(alloc, view, &historical, command) catch |err| {
                if (entry.budget) |value| if (value.denied()) return error.ResourceBudgetExceeded;
                return err;
            };
            entry.prepared = .{ .native = prepared };
        } else {
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            var read = try core.store.beginReadTxn();
            defer read.abort();
            try jobs.requireReserved(&read, binding);
            var command: if (authority == .native) Command else @import("relational_index_raft_control.zig").Command = .{ .effect = .reserved, .binding = binding };
            command.binding.phase = .fencing;
            const Preparation = if (authority == .native) Prepared else @import("relational_index_raft_control.zig").Prepared;
            const prepared = Preparation.control(alloc, view, &read, command) catch |err| {
                if (entry.budget) |value| if (value.denied()) return error.ResourceBudgetExceeded;
                return err;
            };
            entry.prepared = if (authority == .native) .{ .native = prepared } else .{ .raft = prepared };
        }
        try context.ensureActive();
        if (entry.budget) |*value| _ = value.releaseUnusedCredit();
        self.mutex.lockUncancelable(io);
        if (self.epoch == epoch) {
            entry.retain();
            self.entry = entry;
        }
        self.compilations +|= 1;
        self.finish();
        self.mutex.unlock(io);
        return .{ .entry = entry };
    }
};

const Artifacts = @import("relational_index_preparation.zig").PreparedCandidate;

pub const Prepared = struct {
    pub const ApplyOutcome = Outcome;
    pub const max_manifest_bytes = inputs.max_bytes;

    alloc: Allocator,
    command: Command,
    artifacts: ?Artifacts = null,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn validate(self: *const Prepared) !void {
        try self.command.validate();
    }

    pub fn namespace(self: *const Prepared) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }

    pub fn validateReplay(self: *const Prepared, position: Native) !void {
        try (Frame{ .effect = .{ .control = self.command }, .position = position }).validate();
    }

    pub fn replayDuplicateOutcome(self: *const Prepared, position: Native) Outcome {
        return .{ .binding = self.command.binding, .position = position, .duplicate = true };
    }

    /// Called only under the shared DB mutation/apply/outbox pipeline.
    pub fn stage(self: *Prepared, core: anytype, txn: anytype, replay: ?Native) !Outcome {
        return self.stageImpl(core, txn, replay, false);
    }

    fn stageImpl(self: *Prepared, core: anytype, txn: anytype, replay: ?Native, comptime canceling: bool) !Outcome {
        const binding = self.command.binding;
        const duplicate = (try jobs.nativeEffectPosition(txn, binding, self.command.effect)) != null;
        switch (self.command.effect) {
            .admitted => {
                const artifacts = if (self.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
                if (canceling)
                    _ = try jobs.stageCanceledBeginWithReplay(txn, binding, &artifacts.candidate.inputs, replay)
                else
                    _ = try jobs.stageBeginWithReplay(txn, binding, &artifacts.candidate.inputs, replay);
            },
            .fenced => {
                var manager = try core.initTxnManager();
                defer manager.deinit();
                if (canceling)
                    _ = try jobs.stageCanceledFencedWithReplay(txn, &manager, binding, replay)
                else
                    _ = try jobs.stageNativeFencedWithReplay(txn, &manager, binding, replay);
            },
            .reserved => {
                const artifacts = if (self.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
                var manager = try core.initTxnManager();
                defer manager.deinit();
                if (canceling)
                    _ = try jobs.stageCanceledReservationWithReplay(txn, &manager, binding, &artifacts.candidate, &artifacts.addition, replay)
                else
                    _ = try jobs.stageReservationWithReplay(txn, &manager, binding, &artifacts.candidate, &artifacts.addition, replay);
            },
        }
        return .{ .binding = binding, .position = (try jobs.nativeEffectPosition(txn, binding, self.command.effect)) orelse return error.InvalidPrivateIndexNativeReceipt, .duplicate = duplicate };
    }

    pub fn hasPublicationReceipt(self: *const Prepared) bool {
        return self.command.effect != .admitted;
    }

    pub fn populateReceipt(self: *const Prepared, txn: anytype, outcome: *Outcome) !void {
        outcome.receipt = try jobs.nativeReceipt(txn, self.command.binding, self.command.effect);
    }

    pub fn deinit(self: *Prepared) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        if (self.command.inputs_b64) |bytes| self.alloc.free(bytes);
        if (self.artifacts) |*value| value.deinit();
        self.* = undefined;
    }

    /// Physical allocation comes from the actual local immutable catalog, not
    /// from a coordinator-selected counter. Compile before taking the apply lock.
    pub fn admission(alloc: Allocator, before: registry.SchemaView, txn: anytype, header: Admission, manifest: []const u8) !Prepared {
        var artifacts = try Artifacts.init(alloc, before, txn, manifest, .native);
        errdefer artifacts.deinit();
        if (!std.mem.eql(u8, &inputs.digest(artifacts.candidate.next.value), &header.candidate_digest) or
            !std.mem.eql(u8, &inputs.digest(artifacts.candidate.reservation.value), &header.reservation_digest)) return error.InvalidIndexPublicationCandidate;
        const binding = try jobs.prepare(header.id, header.plan_digest, header.owner_digest, header.ordinal, header.fence, .native, &artifacts.candidate, &artifacts.addition);
        const b64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(manifest.len));
        _ = std.base64.standard.Encoder.encode(b64, manifest);
        return .{ .alloc = alloc, .command = .{ .effect = .admitted, .binding = binding, .inputs_b64 = b64 }, .artifacts = artifacts };
    }

    pub fn control(alloc: Allocator, before: registry.SchemaView, txn: anytype, command: Command) !Prepared {
        try command.validate();
        var prepared: Prepared = .{ .alloc = alloc, .command = command };
        prepared.command.inputs_b64 = null;
        errdefer prepared.deinit();
        if (command.inputs_b64) |b64| {
            const length = std.base64.standard.Decoder.calcSizeForSlice(b64) catch return error.InvalidPrivateIndexInputs;
            if (length > inputs.max_bytes) return error.InvalidPrivateIndexInputs;
            const bytes = try alloc.alloc(u8, length);
            defer alloc.free(bytes);
            std.base64.standard.Decoder.decode(bytes, b64) catch return error.InvalidPrivateIndexInputs;
            if (!std.mem.eql(u8, &inputs.digest(bytes), &command.binding.inputs_digest)) return error.InvalidPrivateIndexInputs;
            prepared.artifacts = try Artifacts.init(alloc, before, txn, bytes, .native);
            prepared.command.inputs_b64 = try alloc.dupe(u8, b64);
        } else if (command.effect == .reserved) {
            var manifest = try inputs.Prepared.load(alloc, txn, command.binding.inputs_digest);
            defer manifest.deinit();
            prepared.artifacts = try Artifacts.init(alloc, before, txn, manifest.bytes, .native);
        }
        if (prepared.artifacts) |*artifacts| {
            const b = command.binding;
            const actual = try jobs.prepare(b.id, b.plan_digest, b.owner_digest, b.ordinal, b.fence, .native, &artifacts.candidate, &artifacts.addition);
            if (!std.mem.eql(u8, &try actual.encode(), &try b.encode())) return error.PrivateIndexOwnerJobChanged;
        }
        return prepared;
    }

    /// Serialize the large immutable prefix off-lock. Only the locally assigned
    /// sequence (at most 20 digits) is filled after transactional staging.
    pub fn preparePayload(self: *Prepared) !void {
        try prepareNativePayload(self, "control", self.command, max_control_wire_bytes);
    }

    pub fn finalizePayload(self: *Prepared, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Drain and burn controls have a distinct wire effect and a receipt-free
/// result. Reusing ordinary command preparation does not grant cancellation:
/// each actual staged primitive requires the exact persisted intent.
pub const PreparedCancellationControl = struct {
    pub const ApplyOutcome = CleanupOutcome;
    alloc: Allocator,
    control: Prepared,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, txn: anytype, prepared: Prepared) !PreparedCancellationControl {
        try @import("relational_index_cancellation_intent.zig").requireCanceled(txn, prepared.command.binding);
        return .{ .alloc = alloc, .control = prepared };
    }
    pub fn deinit(self: *PreparedCancellationControl) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.control.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedCancellationControl) !void {
        try self.control.validate();
    }
    pub fn namespace(self: *const PreparedCancellationControl) @FieldType(Native, "namespace") {
        return self.control.namespace();
    }
    pub fn validateReplay(self: *const PreparedCancellationControl, position: Native) !void {
        try (Frame{ .effect = .{ .cancel_control = self.control.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedCancellationControl, position: Native) CleanupOutcome {
        return .{ .binding = self.control.command.binding, .position = position, .duplicate = true };
    }
    pub fn stage(self: *PreparedCancellationControl, core: anytype, txn: anytype, replay: ?Native) !CleanupOutcome {
        const result = try self.control.stageImpl(core, txn, replay, true);
        return .{ .binding = result.binding, .position = result.position, .duplicate = result.duplicate };
    }
    pub fn hasPublicationReceipt(_: *const PreparedCancellationControl) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedCancellationControl, _: anytype, _: *CleanupOutcome) !void {}
    pub fn preparePayload(self: *PreparedCancellationControl) !void {
        try prepareNativePayload(self, "cancel_control", self.control.command, max_control_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedCancellationControl, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Cancellation intent preparation neither loads a schema nor compiles a
/// candidate. Physical authority and the actual existing job are checked in
/// the committing snapshot by stageCancellationIntentWithReplay.
pub const PreparedCancellation = struct {
    pub const ApplyOutcome = CancellationOutcome;
    alloc: Allocator,
    intent: Admission,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, intent: Admission) !PreparedCancellation {
        try intent.validate();
        return .{ .alloc = alloc, .intent = intent };
    }

    pub fn deinit(self: *PreparedCancellation) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }

    pub fn validate(self: *const PreparedCancellation) !void {
        try self.intent.validate();
    }
    pub fn namespace(self: *const PreparedCancellation) @FieldType(Native, "namespace") {
        return self.intent.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedCancellation, position: Native) !void {
        try (Frame{ .effect = .{ .cancel_intent = self.intent }, .position = position }).validate();
    }

    pub fn replayDuplicateOutcome(self: *const PreparedCancellation, position: Native) CancellationOutcome {
        return .{ .intent = self.intent, .position = position, .duplicate = true };
    }

    pub fn stage(self: *PreparedCancellation, _: anytype, txn: anytype, replay: ?Native) !CancellationOutcome {
        const staged = try jobs.stageCancellationIntentWithReplay(txn, self.intent, replay);
        return .{ .intent = staged.record.intent, .position = staged.record.position.native, .duplicate = staged.duplicate };
    }
    pub fn stageRaft(self: *PreparedCancellation, _: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        if (owner_group == 0 or owner_group != self.intent.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try jobs.stageCancellationIntentAt(txn, self.intent, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }

    pub fn hasPublicationReceipt(_: *const PreparedCancellation) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedCancellation, _: anytype, _: *CancellationOutcome) !void {}

    pub fn preparePayload(self: *PreparedCancellation) !void {
        try prepareNativePayload(self, "cancel_intent", self.intent, max_cancellation_wire_bytes);
    }

    pub fn finalizePayload(self: *PreparedCancellation, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Prepared from the actual inspected durable job, never an API-selected
/// physical allocation. Stage rechecks intent, drain and both burned catalogs.
pub const PreparedCleanup = struct {
    pub const ApplyOutcome = CleanupOutcome;
    alloc: Allocator,
    binding: jobs.Binding,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, binding: jobs.Binding) !PreparedCleanup {
        _ = try binding.encode();
        if (binding.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
        return .{ .alloc = alloc, .binding = binding };
    }
    pub fn deinit(self: *PreparedCleanup) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedCleanup) !void {
        _ = try self.binding.encode();
        if (self.binding.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
    }
    pub fn namespace(self: *const PreparedCleanup) @FieldType(Native, "namespace") {
        return self.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedCleanup, position: Native) !void {
        try (Frame{ .effect = .{ .cancel_cleanup = self.binding }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedCleanup, position: Native) CleanupOutcome {
        return .{ .binding = self.binding, .position = position, .duplicate = true };
    }
    pub fn stage(self: *PreparedCleanup, core: anytype, txn: anytype, replay: ?Native) !CleanupOutcome {
        var manager = try core.initTxnManager();
        defer manager.deinit();
        const result = try jobs.stageCancellationCleanupWithReplay(txn, &manager, self.binding, replay);
        return .{ .binding = self.binding, .position = result.position, .duplicate = result.duplicate };
    }
    pub fn stageRaft(self: *PreparedCleanup, core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        if (self.binding.authority != .raft or owner_group == 0 or owner_group != self.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        var manager = try core.initTxnManager();
        defer manager.deinit();
        _ = try jobs.stageCancellationCleanupAt(txn, &manager, self.binding, .{ .raft = .{ .term = entry.term, .index = entry.index } });
    }
    pub fn hasPublicationReceipt(_: *const PreparedCleanup) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedCleanup, _: anytype, _: *CleanupOutcome) !void {}
    pub fn preparePayload(self: *PreparedCleanup) !void {
        if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
        try prepareNativePayload(self, "cancel_cleanup", self.binding, max_cancellation_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedCleanup, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
    }
};

/// Borrow the already bounded deletion selection. Committed-entry replay uses
/// these selected keys, not a new time-limited scan on each follower. This is
/// local execution machinery, not a public cleanup grant or a wire envelope.
pub const PreparedCancellationGc = struct {
    binding: jobs.Binding,
    work: union(enum) {
        artifacts: *@import("relational_index_gc.zig").Page,
        claims: *private_claims.GcPage,
        artifact_selection_applied: *const @import("relational_index_gc.zig").Page.Replay,
        claim_selection_applied: *const private_claims.GcPage.Replay,
    },

    pub fn validate(self: *const @This()) !void {
        _ = try self.binding.encode();
        if (self.binding.authority != .raft or self.binding.phase != .reserved) return error.InvalidPrivateIndexOwnerJob;
        switch (self.work) {
            .artifacts => |page| if (page.id.mapKey() != self.binding.index.mapKey()) return error.PrivateIndexOwnerJobChanged,
            .claims => |page| if (!std.meta.eql(page.binding, self.binding)) return error.PrivateIndexOwnerJobChanged,
            .artifact_selection_applied => |command| {
                try command.validate();
                if (!std.meta.eql(command.binding, self.binding)) return error.PrivateIndexOwnerJobChanged;
            },
            .claim_selection_applied => |command| {
                try command.validate();
                if (!std.meta.eql(command.binding, self.binding)) return error.PrivateIndexOwnerJobChanged;
            },
        }
    }

    pub fn checkWriteConflicts(self: *const @This(), core: anytype) !void {
        try self.validate();
        if (self.work == .artifacts) {
            var manager = try core.initTxnManager();
            defer manager.deinit();
            try manager.checkOrdinaryWriteConflict(&@import("relational_index_maintenance_contract.zig").controlKey(self.binding.index));
        }
    }

    pub fn stageRaft(self: *@This(), core: anytype, txn: anytype, owner_group: u64, entry: @import("apply_receipts.zig").OrderedApplyReceipt) !void {
        try self.validate();
        if (owner_group == 0 or owner_group != self.binding.fence.owner_group_id) return error.PrivateIndexOwnerJobChanged;
        const prior = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        if (try @import("apply_receipts.zig").orderedApplyDisposition(prior, entry) != .apply) return error.InvalidControlReceiptPosition;
        if (prior) |value| if (entry.index <= value.index or entry.term < value.term) return error.InvalidControlReceiptPosition;
        _ = try jobs.requireCancellationCleanup(txn, self.binding);
        switch (self.work) {
            .artifacts => |page| try page.stage(core, txn),
            .claims => |page| try page.stage(txn),
            .artifact_selection_applied => |command| try command.requireApplied(txn),
            .claim_selection_applied => |command| try command.requireApplied(txn),
        }
    }
};

fn prepareNativePayload(prepared: anytype, comptime member: []const u8, body: anytype, maximum: usize) !void {
    // This buffer has no I/O sink: Allocating's WriteFailed means allocation
    // failure. Preserve OOM semantics for callers and exhaustive fault checks.
    return prepareNativePayloadAllocating(prepared, member, body, maximum) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}

fn prepareNativePayloadAllocating(prepared: anytype, comptime member: []const u8, body: anytype, maximum: usize) !void {
    try prepared.validate();
    if (prepared.payload != null) return;
    var writer: std.Io.Writer.Allocating = .init(prepared.alloc);
    defer writer.deinit();
    try writer.writer.writeAll("{\"schema_version\":");
    try std.json.Stringify.value(envelope_version, .{}, &writer.writer);
    try writer.writer.writeAll(",\"request\":{},\"index_owner_control\":{\"effect\":{\"" ++ member ++ "\":");
    try std.json.Stringify.value(body, .{}, &writer.writer);
    try writer.writer.writeAll("},\"position\":{\"namespace\":");
    try std.json.Stringify.value(prepared.namespace(), .{}, &writer.writer);
    try writer.writer.writeAll(",\"sequence\":");
    prepared.prefix_len = writer.written().len;
    if (prepared.prefix_len + 23 > maximum) return error.InvalidPrivateIndexInputs;
    try writer.writer.splatByteAll(' ', 23);
    prepared.payload = try writer.toOwnedSlice();
}

fn finalizeNativePayload(prepared: anytype, position: Native) ![]const u8 {
    try position.validate();
    if (!position.namespace.eql(prepared.namespace())) return error.IdentityNamespaceMismatch;
    const bytes = prepared.payload orelse return error.InvalidPrivateIndexOwnerJob;
    const digits = try std.fmt.bufPrint(bytes[prepared.prefix_len .. prepared.prefix_len + 20], "{d}", .{position.sequence});
    const end = prepared.prefix_len + digits.len;
    @memcpy(bytes[end .. end + 3], "}}}");
    return bytes[0 .. end + 3];
}
