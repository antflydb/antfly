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
pub const envelope_version = 22;
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

pub const PreparedBuild = struct {
    pub const ApplyOutcome = BuildOutcome;
    pub const checkpoint_key = jobs.key ++ ":build-page";
    pub const checkpoint_len = 176;
    alloc: Allocator,
    page: pages.Page,
    command: pages.Page.PrivateCommand,
    command_digest: [32]u8,
    progress_digest: [32]u8,
    payload: ?[]u8 = null,
    prefix_len: usize = 0,

    /// Takes page ownership only on success.
    pub fn init(alloc: Allocator, page: pages.Page) !PreparedBuild {
        const command = try page.privateCommand();
        return .{ .alloc = alloc, .page = page, .command = command, .command_digest = try command.digest(), .progress_digest = try command.encodedProgressDigest() };
    }
    /// Reuse the compiled reservation across pages. Only the page's source
    /// snapshot and scratch arena are renewed; no per-page schema compilation.
    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, reservation: *const Prepared, budget: pages.Budget) !?PreparedBuild {
        if (reservation.command.effect != .reserved) return error.PrivateIndexOwnerJobChanged;
        const artifacts = if (reservation.artifacts) |*value| value else return error.InvalidPrivateIndexOwnerJob;
        var binding = reservation.command.binding;
        binding.phase = .reserved;
        var page = (try pages.Page.preparePrivate(alloc, io, core, binding, &artifacts.addition, budget)) orelse return null;
        errdefer page.deinit();
        return try init(alloc, page);
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
        var page = try pages.Page.replayPrivate(alloc, core, &artifacts.addition, command);
        errdefer page.deinit();
        return try init(alloc, page);
    }
    pub fn deinit(self: *PreparedBuild) void {
        if (self.payload) |bytes| self.alloc.free(bytes);
        self.page.deinit();
        self.* = undefined;
    }
    pub fn validate(self: *const PreparedBuild) !void {
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
        if (raw.len != checkpoint_len or !std.mem.eql(u8, raw[0..8], "AIBP\x01\x00\x00\x00") or
            !std.mem.allEqual(u8, raw[137..144], 0) or !std.mem.eql(u8, raw[144..176], &hash(raw[0..144]))) return error.InvalidRelationalIndexProgress;
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
        _ = try checkpoint(txn, binding);
    }
    pub fn stage(self: *PreparedBuild, core: anytype, txn: anytype, replay: ?Native) !BuildOutcome {
        const binding = self.command.binding;
        try jobs.requireReserved(txn, binding);
        if (try checkpoint(txn, binding)) |bytes| {
            if (std.mem.eql(u8, bytes[40..72], &self.command_digest)) {
                if (!std.mem.eql(u8, bytes[72..104], &self.progress_digest)) return error.InvalidRelationalIndexProgress;
                const position = (try @import("receipt_position.zig").Position.decode(bytes[104..137])).native;
                if (replay) |expected| if (!std.meta.eql(position, expected)) return error.InvalidControlReceiptPosition;
                return .{ .binding = binding, .page_digest = self.command_digest, .position = position, .duplicate = true };
            }
        }
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try self.page.stage(core, &manager, txn);
        const position: Native = .{ .namespace = self.namespace(), .sequence = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(self.namespace()), if (replay) |value| value.sequence else null) };
        var bytes: [checkpoint_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AIBP\x01\x00\x00\x00");
        @memcpy(bytes[8..40], &hash(&try binding.encode()));
        @memcpy(bytes[40..72], &self.command_digest);
        @memcpy(bytes[72..104], &self.progress_digest);
        @memcpy(bytes[104..137], &try (@import("receipt_position.zig").Position{ .native = position }).encode());
        @memcpy(bytes[144..176], &hash(bytes[0..144]));
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

const Artifacts = struct {
    candidate: candidate_mod.Candidate,
    addition: physical.PrivateAddition,

    fn deinit(self: *Artifacts) void {
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
        const addition = if (job != null and job.?.phase == .reserved)
            try jobs.reopenPhysical(alloc, txn, before, after, view.name)
        else
            try physical.PrivateAddition.init(alloc, before, after, if (existing) |*value| value else null, definitions, view.name);
        return .{ .candidate = candidate, .addition = addition };
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
