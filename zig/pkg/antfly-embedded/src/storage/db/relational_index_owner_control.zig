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
const resources = @import("../resource_manager.zig");
const RequestContext = @import("../../api/operation.zig").RequestContext;
const Waiters = @import("../admission_waiter.zig").Fifo(void);
pub const envelope_version = 23;
pub const max_cancellation_wire_bytes = 32 * 1024;
pub const max_encoded_inputs = std.base64.standard.Encoder.calcSize(inputs.max_bytes);
pub const max_claim_wire_bytes = 6 * private_claims.max_bytes + 128 * 1024;
pub const max_control_wire_bytes = max_encoded_inputs + 32 * 1024;
pub const max_wire_bytes = @max(max_control_wire_bytes, max_claim_wire_bytes, pages.Page.PrivateCommand.max_wire_bytes);
pub const Manifest = inputs.Prepared;

pub const Admission = @import("relational_index_cancellation_intent.zig").Intent;

/// Metadata's immutable owner identity, without physical allocation choices or
/// schema-sized inputs. Status is information, never publication permission.
pub const Identity = struct {
    id: [16]u8,
    plan_digest: [32]u8,
    owner_digest: [32]u8,
    ordinal: u16,
    namespace: @import("doc_identity.zig").Namespace,
    owner_group_id: u64,

    pub fn validate(self: Identity) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.owner_digest, 0) or self.owner_group_id == 0 or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.ordinal >= @import("relational_index_publication_progress.zig").max_owners) return error.InvalidPrivateIndexOwnerJob;
    }
};

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
        if (self.binding.authority != .native or self.binding.phase != .reserved) return error.InvalidPrivateIndexClaimInventory;
        try (BoundedClaims{ .items = self.claims }).validate();
        for (self.claims) |claim| {
            if (claim.schema_version != self.binding.physical_candidate.schema_version) return error.InvalidPrivateIndexClaimInventory;
        }
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
    claims: ClaimCommand,
    build: pages.Page.PrivateCommand,

    pub fn namespace(self: Effect) @FieldType(Native, "namespace") {
        return switch (self) {
            .control, .cancel_control => |value| value.binding.fence.namespace,
            .claims => |value| value.binding.fence.namespace,
            .build => |value| value.binding.fence.namespace,
            .cancel_intent => |value| value.fence.namespace,
            .cancel_cleanup => |value| value.fence.namespace,
        };
    }

    pub fn validate(self: Effect) !void {
        switch (self) {
            .control, .cancel_control => |value| try value.validate(),
            .claims => |value| try value.validate(),
            .build => |value| try value.validate(),
            .cancel_intent => |value| try value.validate(),
            .cancel_cleanup => |value| {
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
    position: Native,
    duplicate: bool,
    claims_added: usize = 0,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    /// Physical delivery acknowledgement only, never a publication signature.
    /// Check a decoded provider result before a receiver can use its cut.
    pub fn validateFor(self: ClaimOutcome, command: ClaimCommand) !void {
        try command.validate();
        const actual = self.binding.encode() catch return error.InvalidPrivateIndexClaimReceipt;
        const expected = try command.binding.encode();
        self.position.validate() catch return error.InvalidPrivateIndexClaimReceipt;
        if (!std.mem.eql(u8, &actual, &expected) or
            !self.position.namespace.eql(command.binding.fence.namespace) or
            self.claims_added > command.claims.len or
            self.duplicate != (self.claims_added == 0) or
            self.replication_pending != (self.error_name != null)) return error.InvalidPrivateIndexClaimReceipt;
    }
};

pub const PreparedClaims = struct {
    pub const ApplyOutcome = ClaimOutcome;
    alloc: Allocator,
    batch: private_claims.Prepared,
    command: ClaimCommand,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    pub fn init(alloc: Allocator, txn: anytype, command: ClaimCommand) !PreparedClaims {
        try command.validate();
        var batch = try private_claims.Prepared.init(alloc, txn, command.binding, command.claims);
        errdefer batch.deinit();
        const owned = try batch.arena.allocator().alloc(integrity.Claim, batch.entries.len);
        for (owned, batch.entries) |*claim, entry| claim.* = try integrity.Claim.decode(&entry.address.claimKey(), entry.value);
        return .{ .alloc = alloc, .batch = batch, .command = .{ .binding = command.binding, .claims = owned } };
    }

    pub fn deinit(self: *PreparedClaims) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.batch.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedClaims) !void {
        try self.command.validate();
    }
    pub fn namespace(self: *const PreparedClaims) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const PreparedClaims, position: Native) !void {
        try (Frame{ .effect = .{ .claims = self.command }, .position = position }).validate();
    }
    pub fn replayDuplicateOutcome(self: *const PreparedClaims, position: Native) ClaimOutcome {
        return .{ .binding = self.command.binding, .position = position, .duplicate = true };
    }
    pub fn stage(self: *PreparedClaims, core: anytype, txn: anytype, replay: ?Native) !ClaimOutcome {
        // Actual receiving range, checked under apply exclusion; no caller-set
        // ownership flag or row owner's routing assumption is accepted.
        for (self.batch.entries) |entry| if (!core.byteRange().contains(&entry.address.routing)) return error.KeyOutOfRange;
        const result = try self.batch.stageOrdered(txn, replay);
        return .{ .binding = self.command.binding, .position = result.position, .duplicate = result.duplicate, .claims_added = result.claims_added };
    }
    pub fn hasPublicationReceipt(_: *const PreparedClaims) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedClaims, _: anytype, _: *ClaimOutcome) !void {}
    pub fn preparePayload(self: *PreparedClaims) !void {
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
        if (!std.mem.eql(u8, &try self.binding.encode(), &try command.binding.encode()) or
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

    pub fn init(identity: Identity, binding: jobs.Binding, budget: pages.Budget) !BuildRequest {
        const result: BuildRequest = .{ .identity = identity, .binding_digest = try bindingDigest(binding), .budget = budget };
        try result.requireBinding(binding);
        return result;
    }
    pub fn validate(self: BuildRequest) !void {
        try self.identity.validate();
        try self.budget.validate();
        if (std.mem.allEqual(u8, &self.binding_digest, 0)) return error.InvalidPrivateIndexOwnerJob;
    }
    pub fn requireBinding(self: BuildRequest, binding: jobs.Binding) !void {
        try self.validate();
        const identity = self.identity;
        if (binding.phase != .reserved or binding.authority != .native or
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

    pub fn validateFor(self: BuildResponse, request: BuildRequest) !void {
        switch (self) {
            .complete => |value| try value.validateFor(request),
            .advanced => |value| {
                try request.requireBinding(value.binding);
                try value.position.validate();
                if (!value.position.namespace.eql(request.identity.namespace) or std.mem.allEqual(u8, &value.page_digest, 0) or
                    value.replication_pending != (value.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
            },
        }
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
        if (reservation.command.effect != .reserved) return error.PrivateIndexOwnerJobChanged;
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        var binding = reservation.command.binding;
        binding.phase = .reserved;
        var page = (try pages.Page.preparePrivateClaims(alloc, io, core, binding, &artifacts.addition, budget, context, .{ .scope = .{ .table_name = table_name, .generation = artifacts.unique.generation, .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = artifacts.unique.tuple.fingerprint, .nulls_not_distinct = artifacts.unique.nulls_not_distinct }, .tuple = artifacts.unique.tuple })) orelse return null;
        errdefer page.deinit();
        var result = try init(alloc, page);
        result.context = context;
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
        var artifacts = try Artifacts.init(alloc, view, &read, manifest.bytes);
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
        if (position != .native) return error.InvalidControlReceiptPosition;
        try position.requireNamespace(binding.fence.namespace);
        const state = try source_authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        if (position.native.sequence > state.sequence) return error.InvalidControlReceiptPosition;
        const progress = try txn.get(&pages.progressKey(binding.index));
        if (!std.mem.eql(u8, bytes[72..104], &hash(progress))) return error.InvalidRelationalIndexProgress;
        return bytes;
    }
    pub fn verifyRecovery(txn: anytype, binding: jobs.Binding) !void {
        const bytes = try checkpoint(txn, binding);
        try private_claims.SourceStream.verifyRecovery(txn, binding, if (bytes) |value| (try @import("receipt_position.zig").Position.decode(value[104..137])).native else null, if (bytes) |value| value[72..104].* else null, if (bytes) |value| value[144..176].* else null);
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
    pub fn stage(self: *PreparedBuild, core: anytype, txn: anytype, replay: ?Native) !BuildOutcome {
        try self.context.ensureActive();
        const binding = self.command.binding;
        try jobs.requireReserved(txn, binding);
        if (try checkpoint(txn, binding)) |bytes| {
            if (std.mem.eql(u8, bytes[40..72], &self.command_digest)) {
                if (!std.mem.eql(u8, bytes[72..104], &self.progress_digest)) return error.InvalidRelationalIndexProgress;
                const position = (try @import("receipt_position.zig").Position.decode(bytes[104..137])).native;
                _ = try private_claims.SourceStream.requireLatest(txn, binding, self.command.source, position, self.progress_digest, bytes[144..176].*);
                if (replay) |expected| if (!std.meta.eql(position, expected)) return error.InvalidControlReceiptPosition;
                return .{ .binding = binding, .page_digest = self.command_digest, .position = position, .duplicate = true };
            }
        }
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try self.page.stage(core, &manager, txn);
        const position: Native = .{ .namespace = self.namespace(), .sequence = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.namespace()), if (replay) |value| value.sequence else null) };
        try self.claim_batch.stage(txn, binding, position);
        var bytes: [checkpoint_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AIBP\x02\x00\x00\x00");
        @memcpy(bytes[8..40], &hash(&try binding.encode()));
        @memcpy(bytes[40..72], &self.command_digest);
        @memcpy(bytes[72..104], &self.progress_digest);
        @memcpy(bytes[104..137], &try (@import("receipt_position.zig").Position{ .native = position }).encode());
        @memcpy(bytes[144..176], self.claim_batch.header[288..320]);
        @memcpy(bytes[176..208], &hash(bytes[0..176]));
        try txn.put(checkpoint_key, &bytes);
        return .{ .binding = binding, .page_digest = self.command_digest, .position = position, .duplicate = false };
    }
    pub fn hasPublicationReceipt(_: *const PreparedBuild) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedBuild, _: anytype, _: *BuildOutcome) !void {}
    pub fn preparePayload(self: *PreparedBuild) !void {
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
        prepared: ?Prepared = null,

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
            if (self.prepared) |*value| value.deinit();
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
            return &self.entry.prepared.?;
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
        try context.ensureActive();
        try request.requireBinding(binding);
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
            if (self.entry) |entry| if (std.mem.eql(u8, &entry.digest, &request.binding_digest)) {
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
        const entry = try Entry.create(backing, manager, request.binding_digest);
        errdefer entry.release();
        const alloc = if (entry.budget) |*value| value.allocator() else backing;
        var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
        defer view.release();
        {
            var read = try core.store.beginReadTxn();
            defer read.abort();
            try jobs.requireReserved(&read, binding);
            var command: Command = .{ .effect = .reserved, .binding = binding };
            command.binding.phase = .fencing;
            entry.prepared = Prepared.control(alloc, view, &read, command) catch |err| {
                if (entry.budget) |value| if (value.denied()) return error.ResourceBudgetExceeded;
                return err;
            };
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

const Artifacts = struct {
    candidate: candidate_mod.Candidate,
    addition: physical.PrivateAddition,
    unique: struct { tuple: @import("relational_index_keys.zig").TuplePlan, generation: integrity.Generation, nulls_not_distinct: bool },

    fn deinit(self: *Artifacts) void {
        self.unique.tuple.deinit();
        self.addition.deinit();
        self.candidate.deinit();
    }

    fn init(alloc: Allocator, before: registry.SchemaView, txn: anytype, bytes: []const u8) !Artifacts {
        const view = try inputs.decode(bytes);
        const namespace = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
        _ = try source_authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(namespace));
        var candidate = try candidate_mod.Candidate.prepare(alloc, namespace.table_id, view.before_json, view.after_json, view.predecessor, view.name);
        errdefer candidate.deinit();
        var after_schema = try schema_api.parseValidatedTableSchema(alloc, view.after_json);
        defer after_schema.deinit(alloc);
        const runtime = try schema_api.deriveRuntimeTableSchema(alloc, after_schema);
        const epoch = registry.Epoch.createOwned(alloc, runtime) catch |err| {
            @import("../schema.zig").freeSchema(alloc, runtime);
            return err;
        };
        var after: registry.SchemaView = .{ .epoch = epoch };
        defer after.release();
        var declaration_arena = std.heap.ArenaAllocator.init(alloc);
        defer declaration_arena.deinit();
        const definitions = (try after_schema.relationalIndexDefinitions(declaration_arena.allocator())) orelse return error.InvalidIndexPublicationCandidate;
        var existing = try physical.load(alloc, txn);
        defer if (existing) |*value| value.deinit();
        const job = try jobs.load(txn);
        var addition = if (job != null and job.?.phase == .reserved)
            try jobs.reopenPhysical(alloc, txn, before, after, view.name)
        else
            try physical.PrivateAddition.init(alloc, before, after, if (existing) |*value| value else null, definitions, view.name);
        errdefer addition.deinit();
        const rules = try after_schema.relationalUniqueDefinitions(declaration_arena.allocator());
        const unique = for (rules) |rule| {
            if (std.mem.eql(u8, rule.name, view.name)) break rule;
        } else return error.InvalidIndexPublicationCandidate;
        if (unique.deferrable or unique.timing != .immediate or unique.expressions.len != 0 or
            unique.where_expressions.len != 0 or unique.without_overlaps_period != null) return error.InvalidIndexPublicationCandidate;
        const keys = if (unique.keys.len != 0) unique.keys else blk: {
            const result = try declaration_arena.allocator().alloc(@import("../relational_index.zig").RelationalIndexKey, unique.columns.len);
            for (result, unique.columns) |*entry, column| entry.* = .{ .column = column };
            break :blk result;
        };
        const generation = (candidate.next.catalog.find(.unique, view.name) orelse return error.InvalidIndexPublicationCandidate).generation;
        const allocation = try @import("relational_integrity_catalog.zig").privateIndexAllocationFromReservation(candidate.reservation.value, inputs.digest(candidate.reservation.value), namespace.table_id);
        if (!std.mem.eql(u8, &allocation.generation, &generation)) return error.InvalidIndexPublicationCandidate;
        const tuple = try @import("relational_index_keys.zig").TuplePlan.init(alloc, after.tableSchema().*, after.physicalLayout(), keys);
        return .{ .candidate = candidate, .addition = addition, .unique = .{ .tuple = tuple, .generation = generation, .nulls_not_distinct = unique.nulls_not_distinct } };
    }
};

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
        var artifacts = try Artifacts.init(alloc, before, txn, manifest);
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
            prepared.artifacts = try Artifacts.init(alloc, before, txn, bytes);
            prepared.command.inputs_b64 = try alloc.dupe(u8, b64);
        } else if (command.effect == .reserved) {
            var manifest = try inputs.Prepared.load(alloc, txn, command.binding.inputs_digest);
            defer manifest.deinit();
            prepared.artifacts = try Artifacts.init(alloc, before, txn, manifest.bytes);
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
        return .{ .intent = staged.record.intent, .position = staged.record.position, .duplicate = staged.duplicate };
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
        try (Effect{ .cancel_cleanup = binding }).validate();
        return .{ .alloc = alloc, .binding = binding };
    }
    pub fn deinit(self: *PreparedCleanup) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedCleanup) !void {
        try (Effect{ .cancel_cleanup = self.binding }).validate();
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
    pub fn hasPublicationReceipt(_: *const PreparedCleanup) bool {
        return false;
    }
    pub fn populateReceipt(_: *const PreparedCleanup, _: anytype, _: *CleanupOutcome) !void {}
    pub fn preparePayload(self: *PreparedCleanup) !void {
        try prepareNativePayload(self, "cancel_cleanup", self.binding, max_cancellation_wire_bytes);
    }
    pub fn finalizePayload(self: *PreparedCleanup, position: Native) ![]const u8 {
        return finalizeNativePayload(self, position);
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
