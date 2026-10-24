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

//! Coordinated constraint activation. Coverage binds the immutable active
//! generation set and exact owned range, not mutable schema declaration state.
//! New primary mutations stay gated until ALL table owners report coverage.
//! A page's source-row guards, claims/references and progress transition join
//! the same durable distributed transaction. No progress-only best effort ack.
const std = @import("std");
const integrity = @import("relational_integrity.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const rows = @import("relational_rows.zig");
const ranges = @import("range_state.zig");
const transactions = @import("../transactions.zig");
const Allocator = std.mem.Allocator;
pub const key = @import("relational_integrity_activation_contract.zig").key;
const header_len = @import("relational_integrity_activation_contract.zig").header_len;
const max_cursor_bytes = @import("relational_integrity_activation_contract.zig").max_cursor_bytes;
const full_range = @as([8]u8, @splat(0));

const digest = @import("relational_integrity_activation_contract.zig").digest;

pub const generationSet = @import("relational_integrity_activation_contract.zig").generationSet;

pub const hasActive = @import("relational_integrity_activation_contract.zig").hasActive;

const hasKind = @import("relational_integrity_activation_contract.zig").hasKind;

fn optional(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn ownership(txn: anytype) !integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly constraint coverage owner v1");
    const namespace = try optional(txn, &@import("../internal_keys.zig").identity_namespace_key) orelse return error.CoordinatedConstraintsRequireTableIdentity;
    state.update(namespace);
    state.update((try @import("online_integrity_shadow.zig").rawRange(txn)) orelse (try optional(txn, ranges.range_key)) orelse &full_range);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn routingKey(alloc: Allocator, txn: anytype) ![]const u8 {
    const range = try ranges.decodeRangeAlloc(alloc, (try optional(txn, ranges.range_key)) orelse &full_range);
    defer alloc.free(range.end);
    return range.start;
}

pub const State = @import("relational_integrity_activation_contract.zig").State;
pub const Phase = @import("relational_integrity_activation_contract.zig").Phase;
pub const Progress = @import("relational_integrity_activation_contract.zig").Progress;
const firstPhase = @import("relational_integrity_activation_contract.zig").firstPhase;
const nextPhase = @import("relational_integrity_activation_contract.zig").nextPhase;

pub fn status(txn: anytype, catalog: catalog_mod.Catalog) !Progress {
    const owner = try ownership(txn);
    if (!hasActive(catalog)) return .{
        .generation_set = generationSet(catalog),
        .owner = owner,
        .schema_version = catalog.schema_version,
        .state = .enforced,
        .phase = .check,
    };
    if (try optional(txn, key)) |bytes| {
        var progress = try Progress.decode(bytes);
        if (progress.matches(catalog, owner)) {
            progress.schema_version = catalog.schema_version;
            return progress;
        }
    }
    return .{ .generation_set = generationSet(catalog), .owner = owner, .schema_version = catalog.schema_version, .phase = firstPhase(catalog) };
}

/// Must stage in the schema/catalog/outbox commit using the durable table
/// catalog's authoritative row count/has-data bit. A missing proof is NEVER
/// considered ready outside this atomic empty-table bootstrap.
pub fn stageSchema(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, row_count: u64) !void {
    if (!hasActive(catalog)) {
        try txn.delete(key);
        return;
    }
    var progress = try status(txn, catalog);
    const previous = try optional(txn, key);
    const matched = if (previous) |bytes| (try Progress.decode(bytes)).matches(catalog, progress.owner) else false;
    if (!matched and row_count == 0) progress.state = .enforced;
    const encoded = try progress.encode(alloc);
    defer alloc.free(encoded);
    if (previous == null or !std.mem.eql(u8, previous.?, encoded)) try txn.put(key, encoded);
}

pub fn requireReady(txn: anytype, catalog: catalog_mod.Catalog) !void {
    if (!hasActive(catalog)) return;
    switch ((try status(txn, catalog)).state) {
        .enforced => {},
        .validating => return error.ConstraintActivationInProgress,
        .invalid => return error.ConstraintActivationFailed,
    }
}

/// Used only after the exact catalog addition and native seal agree.
fn carriedIndexProgress(before: catalog_mod.Catalog, after: catalog_mod.Catalog, owner: integrity.Digest, previous: ?[]const u8) !Progress {
    var progress: Progress = .{ .generation_set = generationSet(before), .owner = owner, .schema_version = before.schema_version, .phase = firstPhase(before) };
    if (previous) |bytes| {
        const current = try Progress.decode(bytes);
        if (current.matches(before, owner)) progress = current;
    }
    if (!hasActive(before)) {
        progress.state = .enforced;
        progress.phase = firstPhase(after);
        progress.cursor = "";
        progress.failure = "";
    }
    progress.generation_set = generationSet(after);
    progress.owner = owner;
    progress.schema_version = after.schema_version;
    return progress;
}

/// Owned activation delta for one independently validated UNIQUE addition.
/// This is not metadata installation authority. The native installer must
/// validateBefore in its schema transaction, stage both catalogs, stage this
/// delta, and commit its installed cut/outbox without releasing the fence.
/// Existing invalid/partial coverage is preserved, never promoted by the new
/// index's seal. Already enforced generations need no primary-row rescan.
pub const PreparedIndexPublication = struct {
    alloc: Allocator,
    binding: @import("relational_index_owner_job.zig").Binding,
    seal: @import("relational_index_validation.zig").Seal,
    owner: integrity.Digest,
    catalog_digest: integrity.Digest,
    expected: ?[]u8,
    value: []u8,

    pub fn deinit(self: *@This()) void {
        if (self.expected) |bytes| self.alloc.free(bytes);
        self.alloc.free(self.value);
        self.* = undefined;
    }

    pub fn init(alloc: Allocator, txn: anytype, binding: @import("relational_index_owner_job.zig").Binding, predecessor: []const u8, publication: *const catalog_mod.Update, name: []const u8) !@This() {
        const jobs = @import("relational_index_owner_job.zig");
        const validation = @import("relational_index_validation.zig");
        try jobs.requireReserved(txn, binding);
        var before = try catalog_mod.decode(alloc, predecessor);
        defer before.deinit();
        const added = try catalog_mod.indexAddition(before, publication.catalog, name);
        if (added.retired or !std.mem.eql(u8, &digest(predecessor), &binding.integrity_before) or
            !std.mem.eql(u8, &digest(publication.expected orelse return error.IntegrityCatalogChanged), &binding.integrity_reserved) or
            !std.mem.eql(u8, &digest(publication.value), &binding.integrity_candidate)) return error.IntegrityCatalogChanged;
        const sealed = (try validation.Seal.load(alloc, txn, binding)) orelse return error.PrivateIndexTargetValidationRequired;
        const actual = try validation.SealCommand.prepare(alloc, txn, binding, sealed.command.expected);
        if (!std.meta.eql(actual, sealed.command) or !std.mem.eql(u8, &added.generation, &sealed.command.expected.generation)) return error.InvalidRelationalIndexValidation;
        const owner = try ownership(txn);
        const raw = try optional(txn, key);
        const expected = if (raw) |bytes| try alloc.dupe(u8, bytes) else null;
        errdefer if (expected) |bytes| alloc.free(bytes);
        const progress = try carriedIndexProgress(before, publication.catalog, owner, expected);
        const value = try progress.encode(alloc);
        return .{ .alloc = alloc, .binding = binding, .seal = sealed, .owner = owner, .catalog_digest = binding.integrity_candidate, .expected = expected, .value = value };
    }

    /// Called before schema/catalog puts in the same write transaction. No
    /// preparation-time observation substitutes for this predecessor check.
    pub fn validateBefore(self: *const @This(), txn: anytype) !void {
        try @import("relational_index_owner_job.zig").requireReserved(txn, self.binding);
        const validation = @import("relational_index_validation.zig");
        const command = try validation.SealCommand.prepare(self.alloc, txn, self.binding, self.seal.command.expected);
        if (!std.meta.eql(command, self.seal.command)) return error.InvalidRelationalIndexValidation;
        try self.requireSeal(txn);
        try self.requireActivation(txn);
    }

    fn requireSeal(self: *const @This(), txn: anytype) !void {
        const current = (try @import("relational_index_validation.zig").Seal.load(self.alloc, txn, self.binding)) orelse return error.PrivateIndexTargetValidationRequired;
        if (!std.meta.eql(current, self.seal)) return error.InvalidRelationalIndexValidation;
    }

    fn requireActivation(self: *const @This(), txn: anytype) !void {
        const current = try optional(txn, key);
        if (self.expected) |expected| {
            if (!std.mem.eql(u8, current orelse return error.ConstraintActivationChanged, expected)) return error.ConstraintActivationChanged;
        } else if (current != null) return error.ConstraintActivationChanged;
        if (!std.mem.eql(u8, &try ownership(txn), &self.owner)) return error.ConstraintActivationOwnerChanged;
    }

    /// Stages only after the exact candidate catalog, with the source still
    /// fenced. The installed job/cut and outbox are the installer's effects.
    pub fn stage(self: *const @This(), txn: anytype) !void {
        const jobs = @import("relational_index_owner_job.zig");
        const current = (try jobs.load(txn)) orelse return error.PrivateIndexJobRequired;
        if (!std.meta.eql(current, self.binding)) return error.PrivateIndexOwnerJobChanged;
        try jobs.requireActive(txn, self.binding.fence);
        const candidate = try txn.get(catalog_mod.key);
        if (!std.mem.eql(u8, &digest(candidate), &self.catalog_digest)) return error.IntegrityCatalogChanged;
        try self.requireSeal(txn);
        try self.requireActivation(txn);
        try txn.put(key, self.value);
    }
};

/// Repairs retain an exact-value read guard on failed or diagnosed activation.
/// MATCH PARTIAL missing-parent diagnostics cannot be terminal (there is no
/// negative witness lock), but must still admit constraint-checked repairs.
/// Concurrent validation/retry cannot publish coverage from an old cut.
pub fn repairPredicate(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog) !transactions.VersionPredicate {
    if (!hasActive(catalog)) return error.InvalidConstraintActivation;
    const progress = try status(txn, catalog);
    if (progress.state != .invalid and !(progress.state == .validating and std.mem.eql(u8, progress.failure, "ForeignKeyParentMissing"))) return error.InvalidConstraintActivation;
    const current = (try optional(txn, key)) orelse return error.ConstraintActivationChanged;
    return .{ .key = key, .comparison = .exact_value, .expected_value = try alloc.dupe(u8, current) };
}

pub const Command = @import("relational_integrity_activation_contract.zig").Command;

pub const Prepared = struct {
    intent: transactions.WriteIntent,
    predicate: transactions.VersionPredicate,
    inputs: ?transactions.WriteIntent = null,
};

pub fn prepareCommand(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, command: Command) !Prepared {
    return prepareCommandForOwner(alloc, txn, catalog, command, 0);
}

pub fn prepareCommandForOwner(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, command: Command, physical_root: u128) !Prepared {
    if (command.validation != null) return prepareForeignKeyValidationCommand(alloc, txn, catalog, command, physical_root);
    if (!hasActive(catalog)) return error.ConstraintNotFound;
    const current = try optional(txn, key);
    if (command.expected) |expected| {
        if (!std.mem.eql(u8, current orelse return error.ConstraintActivationChanged, expected)) return error.ConstraintActivationChanged;
    } else if (current != null) return error.ConstraintActivationChanged;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    if (!std.mem.eql(u8, command.routing_key, try routingKey(arena.allocator(), txn))) return error.ConstraintActivationOwnerChanged;
    const before = try status(txn, catalog);
    // Malformed proposed bytes are a terminal command rejection; malformed
    // stored progress from status() above remains a storage failure.
    const after = Progress.decode(command.next) catch return error.InvalidConstraintActivationCommand;
    if (!after.matches(catalog, before.owner) or after.schema_version != catalog.schema_version) return error.ConstraintActivationChanged;
    if (command.diagnostic) {
        if (command.retry or before.state != .validating or after.state != .validating or after.phase != before.phase or after.rows_scanned != before.rows_scanned or
            !std.mem.eql(u8, after.cursor, before.cursor) or after.failure.len == 0) return error.InvalidConstraintActivationCommand;
    } else if (command.retry) {
        const initial_phase = firstPhase(catalog);
        if (before.state != .invalid or after.state != .validating or after.phase != initial_phase or after.cursor.len != 0 or after.rows_scanned != 0) return error.InvalidConstraintActivationCommand;
    } else {
        if (before.state != .validating or after.rows_scanned < before.rows_scanned) return error.InvalidConstraintActivationCommand;
        if (after.phase != before.phase) {
            if (after.phase != (nextPhase(catalog, before.phase) orelse return error.InvalidConstraintActivationCommand) or after.state != .validating or after.cursor.len != 0) return error.InvalidConstraintActivationCommand;
        } else if (after.state == .validating and std.mem.order(u8, after.cursor, before.cursor) != .gt) return error.InvalidConstraintActivationCommand;
        if (after.state == .enforced and nextPhase(catalog, before.phase) != null) return error.InvalidConstraintActivationCommand;
    }
    return .{ .intent = .{ .key = key, .value = command.next }, .predicate = .{ .key = key, .comparison = .exact_value, .expected_value = command.expected } };
}

pub const ValidationRecord = @import("relational_integrity_activation_contract.zig").ForeignKeyValidationRecord;
pub const validation_key = @import("relational_integrity_activation_contract.zig").validation_key;
pub const validation_inputs_key = @import("relational_integrity_activation_contract.zig").validation_inputs_key;

/// Fixed definition guards, not permission to publish. Secondary index build
/// coverage may advance independently; validation concerns the primary layout,
/// FK declarations and actual source ownership captured at admission.
pub fn requireValidationDefinition(txn: anytype, binding: @import("relational_schema_owner_contract.zig").Binding) !void {
    const actual_namespace = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual_namespace.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
    _ = try @import("../source_authority.zig").require(txn, binding.authority, @import("online_source_contract.zig").namespaceBytes(actual_namespace));
    // Begin independently verifies complete schema/public bytes. Thereafter
    // the live definition lease and atomic catalog publication let pages use
    // the exact catalog digest and durable epoch instead of rehashing large
    // schema blobs for every bounded transaction.
    const table = @import("table_catalog.zig");
    const facts = try table.Catalog.decode((try optional(txn, table.key)) orelse return error.SchemaValidationJobChanged);
    if (facts.storage_mode != .relational or facts.active_schema_version != binding.before_version or
        !std.mem.eql(u8, &digest((try optional(txn, catalog_mod.key)) orelse return error.SchemaValidationJobChanged), &binding.fence.catalog_digest)) return error.SchemaValidationJobChanged;
    if (!std.mem.eql(u8, &binding.owner, &try ownership(txn))) return error.SchemaValidationJobChanged;
}

/// Validity-only installation already proved the unchanged physical generation
/// set. Preserve enforced coverage while rebinding its logical fingerprint and
/// schema epoch in the same schema/catalog/outbox transaction.
pub fn stageValidatedForeignKey(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, binding: @import("relational_schema_owner_contract.zig").Binding) !void {
    const record = try ValidationRecord.decode((try optional(txn, validation_key)) orelse return error.SchemaValidationJobRequired);
    if (!std.meta.eql(record.binding, binding) or record.progress.state != .enforced or catalog.schema_version != binding.after_version)
        return error.SchemaValidationJobChanged;
    var progress = try Progress.decode((try optional(txn, key)) orelse return error.ConstraintActivationRequired);
    if (progress.state != .enforced or progress.schema_version != binding.before_version or
        !std.mem.eql(u8, &progress.owner, &binding.owner) or !std.mem.eql(u8, &progress.generation_set, &record.progress.generation_set))
        return error.SchemaValidationNotReady;
    progress.schema_version = catalog.schema_version;
    progress.generation_set = generationSet(catalog);
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    try txn.put(key, bytes);
}

fn prepareForeignKeyValidationCommand(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog, command: Command, physical_root: u128) !Prepared {
    const scope = command.validation.?;
    try scope.validate();
    if (scope.binding.authority == .native and (physical_root == 0 or scope.physical_root != physical_root)) return error.SchemaValidationRuntimeRecoveryRequired;
    if (command.retry or command.diagnostic) return error.InvalidConstraintActivationCommand;
    const current_raw = if (try optional(txn, validation_key)) |raw| try alloc.dupe(u8, raw) else null;
    defer if (current_raw) |raw| alloc.free(raw);
    if (command.expected) |expected| {
        if (!std.mem.eql(u8, current_raw orelse return error.ConstraintActivationChanged, expected)) return error.ConstraintActivationChanged;
    } else if (current_raw != null) return error.ConstraintActivationChanged;
    const after = try ValidationRecord.decode(command.next);
    if (!std.meta.eql(after.binding, scope.binding) or after.physical_root != scope.physical_root or !after.progress.matches(catalog, scope.binding.owner)) return error.SchemaValidationJobChanged;
    try requireValidationDefinition(txn, scope.binding);
    const routing = try routingKey(alloc, txn);
    defer alloc.free(routing);
    if (!std.mem.eql(u8, command.routing_key, routing)) return error.ConstraintActivationOwnerChanged;
    if (scope.action == .begin) {
        try @import("relational_integrity_topology.zig").requireUnfenced(txn);
        if (try @import("relational_integrity_topology.zig").completed(txn)) |done| if (done.admission_epoch >= scope.binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
        if (current_raw) |raw| {
            const before = try ValidationRecord.decode(raw);
            if (before.progress.state != .invalid or before.binding.fence.admission_epoch >= scope.binding.fence.admission_epoch or
                std.mem.eql(u8, &before.binding.id, &scope.binding.id)) return error.SchemaValidationJobChanged;
        }
        if (after.revision != 1 or after.progress.state != .validating or after.progress.rows_scanned != 0 or after.progress.cursor.len != 0) return error.InvalidSchemaValidationJob;
        try requireReady(txn, catalog);
        try @import("relational_integrity_generation_admission.zig").requireAcknowledged(txn);
        try @import("relational_integrity_generation_admission.zig").requireReferenceCoverageAcknowledged(txn);
        var candidate = try @import("relational_nullability_job.zig").Candidate.fromInputs(alloc, scope.inputs.bytes, scope.binding, .{});
        defer candidate.deinit();
        const observed = try @import("relational_nullability_job.zig").prepareBinding(txn, &candidate, scope.binding.id, scope.binding.plan_digest, scope.binding.fence, scope.binding.authority);
        var semantic_binding = scope.binding;
        // The immutable source preparation records its observed index head,
        // but FK row validation does not freeze unrelated index build progress.
        semantic_binding.physical_catalog = observed.physical_catalog;
        if (!std.meta.eql(observed, semantic_binding)) return error.SchemaValidationJobChanged;
        return .{ .intent = .{ .key = validation_key, .value = command.next }, .predicate = .{ .key = validation_key, .comparison = .exact_value, .expected_value = command.expected }, .inputs = .{ .key = validation_inputs_key, .value = scope.inputs.bytes } };
    }
    const before = try ValidationRecord.decode(current_raw orelse return error.SchemaValidationJobRequired);
    if (!std.meta.eql(before.binding, scope.binding) or before.physical_root != scope.physical_root or
        after.revision != (std.math.add(u64, before.revision, 1) catch return error.InvalidSchemaValidationJob) or
        (before.progress.state != .validating and !(scope.action == .cancel and before.progress.state == .enforced))) return error.SchemaValidationJobChanged;
    if (after.progress.rows_scanned < before.progress.rows_scanned) return error.InvalidSchemaValidationJob;
    if (scope.action == .cancel or after.progress.state == .invalid) {
        if (after.progress.state != .invalid or after.progress.rows_scanned != before.progress.rows_scanned or
            !std.mem.eql(u8, after.progress.cursor, before.progress.cursor) or
            (scope.action == .cancel and !std.mem.eql(u8, after.progress.failure, "Canceled"))) return error.InvalidSchemaValidationJob;
    } else if (after.progress.state == .validating) {
        if (std.mem.order(u8, after.progress.cursor, before.progress.cursor) != .gt or
            !@import("../internal_keys.zig").isRelationalRowKey(after.progress.cursor)) return error.InvalidSchemaValidationJob;
    }
    return .{ .intent = .{ .key = validation_key, .value = command.next }, .predicate = .{ .key = validation_key, .comparison = .exact_value, .expected_value = command.expected } };
}

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: rows.Page,
    command: Command,
    progress: Progress,
    phase: Phase,
    pub fn deinit(self: *Page) void {
        self.rows.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    /// A deterministic failure is persisted only with the page's original
    /// source-version guards. Abort its failed claim/reference transaction
    /// first; never acknowledge successful coverage for its rejected rows.
    pub fn markFailed(self: *Page, reason: []const u8) !void {
        if (reason.len == 0 or reason.len > 4096) return error.InvalidConstraintActivation;
        self.progress.state = .invalid;
        self.progress.phase = self.phase;
        self.progress.failure = try self.arena.allocator().dupe(u8, reason);
        self.command.next = try self.progress.encode(self.arena.allocator());
    }

    /// Opens one immutable source snapshot and releases the apply fence before
    /// projection. The coordinator enlists every returned row's version guard
    /// with its generated integrity commands AND this page's CAS command.
    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, budget: rows.Budget) !?Page {
        var reader = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            // Validate all physically retained rows, including TTL candidates;
            // only coordinated expiration may retire constrained parents.
            break :blk try rows.Reader.open(alloc, core.store, view, null, .{ .include_primary_digest = true }, 0);
        };
        defer reader.deinit();
        try @import("online_integrity_shadow.zig").requireCatalogMutable(&reader.read);
        const catalog_raw = try optional(&reader.read, catalog_mod.key) orelse return error.ConstraintNotFound;
        var catalog = try catalog_mod.decode(alloc, catalog_raw);
        defer catalog.deinit();
        if (!hasActive(catalog)) return null;
        if (catalog.schema_version != reader.active.version()) return error.ConstraintActivationChanged;
        var progress = try status(&reader.read, catalog);
        const phase = progress.phase;
        if (progress.state != .validating) return null;
        progress.failure = "";
        // Bind the cold projection to the checkpoint in this SAME source
        // snapshot. A large CHECK column must not inflate UNIQUE/FK pages.
        const public = (reader.active.validator() orelse return error.ConstraintNotFound).schema;
        var selected_fields: std.ArrayList([]const u8) = .empty;
        defer selected_fields.deinit(alloc);
        var selected: std.StringHashMapUnmanaged(void) = .empty;
        defer selected.deinit(alloc);
        switch (phase) {
            .unique => {
                var scratch = std.heap.ArenaAllocator.init(alloc);
                defer scratch.deinit();
                for (try public.relationalUniqueDefinitions(scratch.allocator())) |constraint| for (try @import("../../schema/relational_declarations.zig").uniqueFields(scratch.allocator(), reader.active.tableSchema().*, reader.active.physicalLayout(), constraint)) |column| {
                    if (!(try selected.getOrPut(alloc, column)).found_existing) try selected_fields.append(alloc, column);
                };
            },
            .foreign_key => if (public.foreign_keys) |constraints| {
                for (constraints.value) |constraint| for (constraint.child_columns) |column| {
                    if (!(try selected.getOrPut(alloc, column)).found_existing) try selected_fields.append(alloc, column);
                };
            },
            .check => if (reader.active.validator().?.execution.checks) |checks| {
                for (checks.validation_dependency_fields) |column| {
                    if (!(try selected.getOrPut(alloc, column)).found_existing) try selected_fields.append(alloc, column);
                }
            },
        }
        reader.fields = selected_fields.items;
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, reader.lower) == .lt or std.mem.order(u8, progress.cursor, reader.upper) != .lt)) return error.InvalidConstraintActivation;
        try reader.after.appendSlice(alloc, progress.cursor);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const expected = if (try optional(&reader.read, key)) |bytes| try owned.dupe(u8, bytes) else null;
        const routing = try routingKey(owned, &reader.read);
        var page_rows = reader.nextPage(alloc, io, budget) catch |err| {
            if (err != error.RelationalRowResultTooLarge and err != error.RelationalIndexColumnTypeMismatch) return err;
            // A single unprojectable row must not leave an owner retrying
            // forever. Publish a conservative failure CAS with NO source
            // coverage or cursor advancement; ordinary mutations remain
            // fenced until explicit repair/retry resolves the diagnostic.
            progress.state = .invalid;
            progress.failure = @errorName(err);
            progress.cursor = try owned.dupe(u8, progress.cursor);
            const next = try progress.encode(owned);
            const observed = reader.failed_row orelse return error.MissingPrimaryObservation;
            const failed_rows = try owned.alloc(rows.Row, 1);
            failed_rows[0] = observed;
            failed_rows[0].key = try owned.dupe(u8, observed.key);
            return .{
                .arena = arena,
                .rows = .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = failed_rows, .more = true, .records_examined = 0, .output_bytes = 0 },
                .progress = progress,
                .phase = phase,
                .command = .{ .routing_key = routing, .expected = expected, .next = next },
            };
        };
        errdefer page_rows.deinit();
        // This is a proposed successful checkpoint. Its publication still
        // requires the worker's constraint checks and exact checkpoint CAS;
        // a previous retryable diagnostic must not poison successful coverage.
        progress.failure = "";
        progress.rows_scanned = std.math.add(u64, progress.rows_scanned, page_rows.rows.len) catch return error.InvalidConstraintActivation;
        progress.cursor = if (page_rows.more) try owned.dupe(u8, reader.after.items) else "";
        if (!page_rows.more) {
            if (nextPhase(catalog, phase)) |next_phase| progress.phase = next_phase else progress.state = .enforced;
        }
        const next = try progress.encode(owned);
        return .{ .arena = arena, .rows = page_rows, .progress = progress, .phase = phase, .command = .{ .routing_key = routing, .expected = expected, .next = next } };
    }
};

test "relational integrity activation coverage binds generation owner phase and checksum" {
    const alloc = std.testing.allocator;
    var catalog = try catalog_mod.prepare(alloc, null, @splat(1), 1, @splat(2), &.{ .{ .kind = .unique, .name = "id", .fingerprint = @splat(3) }, .{ .kind = .foreign_key, .name = "parent", .fingerprint = @splat(4) } });
    defer catalog.deinit();
    var progress: Progress = .{ .generation_set = generationSet(catalog.catalog), .owner = @splat(5), .schema_version = 1 };
    try std.testing.expect(!progress.readyForReferences());
    progress.phase = .foreign_key;
    try std.testing.expect(progress.readyForReferences());
    progress.failure = "ForeignKeyParentMissing";
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    const decoded = try Progress.decode(bytes);
    try std.testing.expectEqual(State.validating, decoded.state);
    try std.testing.expectEqualStrings("ForeignKeyParentMissing", decoded.failure);
    try std.testing.expect(decoded.readyForReferences());
    try std.testing.expect(decoded.matches(catalog.catalog, @splat(5)));
    try std.testing.expect(!decoded.matches(catalog.catalog, @splat(6)));
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidConstraintActivation, Progress.decode(bytes));
}

test "relational index build pages activation preserves older enforced invalid and partial coverage" {
    const alloc = std.testing.allocator;
    const definitions = [_]catalog_mod.Definition{
        .{ .kind = .unique, .name = "id", .fingerprint = @splat(3) },
        .{ .kind = .foreign_key, .name = "parent", .fingerprint = @splat(4) },
        .{ .kind = .unique, .name = "new_index", .fingerprint = @splat(5) },
    };
    var before = try catalog_mod.prepareWithChecks(alloc, null, @splat(1), 1, @splat(2), definitions[0..2], @splat(9));
    defer before.deinit();
    var after = try catalog_mod.prepareWithChecks(alloc, before.value, @splat(1), 2, @splat(6), &definitions, @splat(9));
    defer after.deinit();
    _ = try catalog_mod.indexAddition(before.catalog, after.catalog, "new_index");
    const owner: integrity.Digest = @splat(7);
    for ([_]State{ .enforced, .invalid, .validating }) |state| {
        const previous: Progress = .{ .generation_set = generationSet(before.catalog), .owner = owner, .schema_version = 1, .state = state, .phase = .foreign_key, .rows_scanned = 11, .cursor = if (state == .enforced) "" else "row11", .failure = if (state == .invalid) "ForeignKeyParentMissing" else "" };
        const bytes = try previous.encode(alloc);
        defer alloc.free(bytes);
        const next = try carriedIndexProgress(before.catalog, after.catalog, owner, bytes);
        try std.testing.expect(next.matches(after.catalog, owner));
        try std.testing.expectEqual(@as(u32, 2), next.schema_version);
        try std.testing.expectEqual(state, next.state);
        try std.testing.expectEqual(previous.phase, next.phase);
        try std.testing.expectEqual(previous.rows_scanned, next.rows_scanned);
        try std.testing.expectEqualStrings(previous.cursor, next.cursor);
        try std.testing.expectEqualStrings(previous.failure, next.failure);
        const different_owner = try carriedIndexProgress(before.catalog, after.catalog, @splat(8), bytes);
        try std.testing.expectEqual(State.validating, different_owner.state);
        try std.testing.expectEqual(@as(u64, 0), different_owner.rows_scanned);
        try std.testing.expectEqualStrings("", different_owner.cursor);
    }
    const missing = try carriedIndexProgress(before.catalog, after.catalog, owner, null);
    try std.testing.expectEqual(State.validating, missing.state);
    try std.testing.expectEqual(firstPhase(before.catalog), missing.phase);
    var empty = try catalog_mod.prepare(alloc, null, @splat(1), 1, @splat(2), &.{});
    defer empty.deinit();
    var only_index = try catalog_mod.prepare(alloc, empty.value, @splat(1), 2, @splat(6), definitions[2..]);
    defer only_index.deinit();
    _ = try catalog_mod.indexAddition(empty.catalog, only_index.catalog, "new_index");
    const ready = try carriedIndexProgress(empty.catalog, only_index.catalog, owner, null);
    try std.testing.expectEqual(State.enforced, ready.state);
    try std.testing.expectEqual(Phase.unique, ready.phase);
    try std.testing.expect(ready.matches(only_index.catalog, owner));
}
