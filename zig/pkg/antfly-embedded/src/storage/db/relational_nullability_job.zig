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

//! Durable, fenced read-side validation jobs. These transaction helpers never
//! commit, authenticate metadata, sign receipts or publish a schema. Callers
//! must abort on any error and join effects with their apply/outbox transaction.
//! Public/generic topology admission remains guarded until the complete owner,
//! metadata, standby and installation protocol is integrated.
const std = @import("std");
const A = std.mem.Allocator;
const schema = @import("../schema.zig");
const api = @import("../../schema/mod.zig");
const compiler = @import("../../schema/relational_declarations.zig");
const logical = @import("../../schema/column_nullability.zig");
const projection = @import("relational_nullability.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const topology = @import("relational_integrity_topology.zig");
const positions = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");
const integrity = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const physical = @import("relational_index_catalog.zig");
const rows = @import("relational_rows.zig");
const transactions = @import("../transactions.zig");
const receipts = @import("apply_receipts.zig");
const Digest = [32]u8;
pub const key = "\x00\x00__metadata__:schema_validation_job:v1";
pub const inputs_key = key ++ ":inputs";
pub const cursor_key = key ++ ":cursor";
const public_key = "\x00\x00__metadata__:schema_json";
const contract = @import("relational_schema_owner_contract.zig");
const max_schema = contract.max_schema;
const max_names = contract.max_names;
const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;

pub const hash = contract.hash;
fn eq(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn optional(txn: anytype, name: []const u8) !?[]const u8 {
    return txn.get(name) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}
fn digestAt(txn: anytype, name: []const u8) !Digest {
    return hash((try optional(txn, name)) orelse "");
}

/// Owned immutable preparation. Compilation is outside serialized apply;
/// admission checks exact bytes/hashes, not a caller's asserted column types.
pub const Candidate = struct {
    pub const max_input_bytes = contract.max_input_bytes;
    alloc: A,
    inputs: []u8,
    native_before: []u8,
    native_after: []u8,
    catalog_after: []u8,
    plan: projection.Plan,
    before_version: u32,
    after_version: u32,

    pub fn deinit(self: *Candidate) void {
        self.alloc.free(self.inputs);
        self.alloc.free(self.native_before);
        self.alloc.free(self.native_after);
        self.alloc.free(self.catalog_after);
        self.plan.deinit();
        self.* = undefined;
    }
    pub fn init(a: A, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, names: []const []const u8, control: projection.Control) !Candidate {
        try control.ensureActive();
        if (table_id == 0 or before_json.len == 0 or before_json.len > max_schema or after_json.len == 0 or after_json.len > max_schema or predecessor.len == 0 or predecessor.len > integrity.max_catalog_bytes)
            return error.InvalidSchemaValidationJob;
        var before = try api.PreparedSchemaDefinition.init(a, before_json);
        defer before.deinit(a);
        var after = try api.PreparedSchemaDefinition.init(a, after_json);
        defer after.deinit(a);
        if (before.runtime_schema.storage_mode != .relational or after.runtime_schema.storage_mode != .relational or
            after.runtime_schema.version != (std.math.add(u32, before.runtime_schema.version, 1) catch return error.InvalidSchemaValidationJob))
            return error.InvalidSchemaValidationJob;
        var layout = try codec.PhysicalLayout.init(a, before.runtime_schema);
        defer layout.deinit();
        var plan = try projection.Plan.init(a, before.runtime_schema, &layout, names, control);
        errdefer plan.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const changes = try scratch.alloc(logical.Change, names.len);
        for (names, changes) |name, *change| change.* = .{ .column = name, .nullable = false };
        const old = try std.json.parseFromSliceLeaky(std.json.Value, scratch, before_json, .{ .parse_numbers = false });
        const next = try std.json.parseFromSliceLeaky(std.json.Value, scratch, after_json, .{ .parse_numbers = false });
        const summary = try logical.verifyTransition(scratch, old, next, changes, .{ .checkpoint = control.checkpoint, .checkpoint_context = control.context });
        if (summary.changed != names.len or summary.tightened != names.len) return error.InvalidSchemaValidationJob;
        var old_catalog = try compiler.compileCatalog(a, before.public_schema, table_id, predecessor);
        defer old_catalog.deinit();
        if (!std.mem.eql(u8, old_catalog.value, predecessor)) return error.IntegrityCatalogChanged;
        var next_catalog = try compiler.compileCatalog(a, after.public_schema, table_id, predecessor);
        defer next_catalog.deinit();
        // NOT NULL does not authorize replacing existing claims/references or
        // silently promoting independently incomplete constraint coverage.
        if (!eq(activation.generationSet(old_catalog.catalog), activation.generationSet(next_catalog.catalog)))
            return error.InvalidSchemaValidationJob;
        const native_before = try schema.serializeSchema(a, before.runtime_schema);
        errdefer a.free(native_before);
        const native_after = try schema.serializeSchema(a, after.runtime_schema);
        errdefer a.free(native_after);
        const catalog_after = try a.dupe(u8, next_catalog.value);
        errdefer a.free(catalog_after);
        const name_json = try std.json.Stringify.valueAlloc(a, plan.names, .{});
        defer a.free(name_json);
        if (name_json.len > max_names) return error.InvalidSchemaValidationJob;
        const components = [_][]const u8{ before_json, after_json, predecessor, name_json };
        var length: usize = 24 + 32;
        for (components) |part| length += part.len;
        const inputs = try a.alloc(u8, length);
        errdefer a.free(inputs);
        @memcpy(inputs[0..8], "ASVI\x01\x00\x00\x00");
        var offset: usize = 24;
        for (components, 0..) |part, i| {
            std.mem.writeInt(u32, inputs[8 + 4 * i ..][0..4], @intCast(part.len), .little);
            @memcpy(inputs[offset..][0..part.len], part);
            offset += part.len;
        }
        @memcpy(inputs[offset..][0..32], &hash(inputs[0..offset]));
        try control.ensureActive();
        return .{ .alloc = a, .inputs = inputs, .native_before = native_before, .native_after = native_after, .catalog_after = catalog_after, .plan = plan, .before_version = before.runtime_schema.version, .after_version = after.runtime_schema.version };
    }
    pub const parts = contract.inputParts;
    pub fn load(a: A, txn: anytype, binding: Binding, control: projection.Control) !Candidate {
        const raw = (try optional(txn, inputs_key)) orelse return error.SchemaValidationJobRequired;
        if (!eq(hash(raw), binding.inputs)) return error.SchemaValidationJobChanged;
        const view = try parts(raw);
        var names = try std.json.parseFromSlice([]const []const u8, a, view[3], .{});
        defer names.deinit();
        var result = try init(a, binding.fence.namespace.table_id, view[0], view[1], view[2], names.value, control);
        errdefer result.deinit();
        try requireCandidate(binding, &result);
        return result;
    }
};

pub const Binding = contract.Binding;

pub fn prepareBinding(txn: anytype, candidate: *const Candidate, id: [16]u8, plan_digest: Digest, fence: topology.Fence, kind: authority.Kind) !Binding {
    const parts = try Candidate.parts(candidate.inputs);
    const result: Binding = .{
        .id = id,
        .plan_digest = plan_digest,
        .fence = fence,
        .authority = kind,
        .owner = try activation.ownership(txn),
        .before_version = candidate.before_version,
        .after_version = candidate.after_version,
        .columns = @intCast(candidate.plan.names.len),
        .before_schema = hash(candidate.native_before),
        .after_schema = hash(candidate.native_after),
        .before_public = hash(parts[0]),
        .after_public = hash(parts[1]),
        .after_catalog = hash(candidate.catalog_after),
        .physical_catalog = try digestAt(txn, physical.head_key),
        .inputs = hash(candidate.inputs),
        .projection_identity = candidate.plan.identity,
    };
    try result.validate();
    try requireCurrent(result, txn);
    return result;
}
pub fn requireCandidate(self: Binding, candidate: *const Candidate) !void {
    try self.validate();
    const parts = try Candidate.parts(candidate.inputs);
    if (self.before_version != candidate.before_version or self.after_version != candidate.after_version or
        self.columns != candidate.plan.names.len or !eq(self.projection_identity, candidate.plan.identity) or
        !eq(self.inputs, hash(candidate.inputs)) or !eq(self.before_schema, hash(candidate.native_before)) or
        !eq(self.after_schema, hash(candidate.native_after)) or !eq(self.before_public, hash(parts[0])) or
        !eq(self.after_public, hash(parts[1])) or !eq(self.after_catalog, hash(candidate.catalog_after)) or
        !eq(self.fence.catalog_digest, hash(parts[2]))) return error.SchemaValidationJobChanged;
}
fn requireNamespace(self: Binding, txn: anytype) !void {
    const actual = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual.eql(self.fence.namespace)) return error.IdentityNamespaceMismatch;
    _ = try authority.require(txn, self.authority, @import("online_source_contract.zig").namespaceBytes(actual));
}
fn requireCurrent(self: Binding, txn: anytype) !void {
    try requireNamespace(self, txn);
    if (!eq(self.owner, try activation.ownership(txn)) or !eq(self.before_schema, try digestAt(txn, schema.schema_key)) or
        !eq(self.before_public, try digestAt(txn, public_key)) or !eq(self.fence.catalog_digest, try digestAt(txn, integrity.key)) or
        !eq(self.physical_catalog, try digestAt(txn, physical.head_key))) return error.SchemaValidationJobChanged;
}

pub const Phase = contract.Phase;
pub const Installation = contract.Installation;
pub const Header = contract.Header;

const precedesOrEqual = contract.precedesOrEqual;
const precedes = contract.precedes;
pub fn load(txn: anytype) !?Header {
    return if (try optional(txn, key)) |bytes| try Header.decode(bytes) else null;
}
fn save(a: A, txn: anytype, header: Header) !void {
    const bytes = try header.encode(a);
    defer a.free(bytes);
    try txn.put(key, bytes);
}
fn requireCommitted(txn: anytype, header: Header) !void {
    try requireNamespace(header.binding, txn);
    const actual: positions.Position = if (header.binding.authority == .native)
        .{ .native = .{ .namespace = header.binding.fence.namespace, .sequence = (try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(header.binding.fence.namespace))).sequence } }
    else blk: {
        const entry = (try receipts.readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
        break :blk .{ .raft = .{ .term = entry.term, .index = entry.index } };
    };
    try actual.validate();
    try precedesOrEqual(header.updated, actual);
}
fn nextPosition(txn: anytype, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !positions.Position {
    try requireNamespace(binding, txn);
    if (binding.authority == .native) {
        if (entry != null) return error.InvalidControlReceiptPosition;
        return .{ .native = .{ .namespace = binding.fence.namespace, .sequence = try authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), null) } };
    }
    const committed = entry orelse return error.InvalidControlReceiptPosition;
    if (try receipts.orderedApplyDisposition(try receipts.readOrderedApplyReceiptTxn(txn), committed) != .apply) return error.InvalidControlReceiptPosition;
    return .{ .raft = .{ .term = committed.term, .index = committed.index } };
}

/// Hot fencing checks are fixed-size and allocation-free. They deny writes;
/// structural validity is not an authenticated validation receipt.
pub fn requireActive(txn: anytype, fence: topology.Fence) !void {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!header.binding.fence.eql(fence) or header.phase == .canceled) return error.SchemaValidationJobChanged;
    try requireNamespace(header.binding, txn);
}
pub fn requireFencing(txn: anytype, fence: topology.Fence) !void {
    try requireActive(txn, fence);
    if ((try load(txn)).?.phase != .fencing) return error.SchemaValidationJobChanged;
}
pub fn requireCanceling(txn: anytype, fence: topology.Fence) !void {
    try requireActive(txn, fence);
    if ((try load(txn)).?.phase != .canceling) return error.SchemaValidationCancellationRequired;
}
fn workHeader(txn: anytype, binding: Binding) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding) or header.phase == .canceling or header.phase == .canceled) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    const actual = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(binding.fence)) return error.IntegrityTopologyChanged;
    // The exclusive owner fence blocks schema/catalog/ownership changes in
    // core.commitSchemaMetadata and the normal topology paths. Recheck small
    // durable facts here; do not rehash multi-MiB schemas on each page apply.
    const facts = try @import("table_catalog.zig").Catalog.decode((try optional(txn, @import("table_catalog.zig").key)) orelse return error.SchemaValidationJobChanged);
    if (facts.storage_mode != .relational or facts.active_schema_version != binding.before_version or
        !eq(binding.physical_catalog, try digestAt(txn, physical.head_key))) return error.SchemaValidationJobChanged;
    return header;
}

/// Preparation guard, not permission to install or release a schema. The
/// publication participant must recheck this exact ready cut under apply.
pub fn requireReady(txn: anytype, binding: Binding) !Header {
    const header = try workHeader(txn, binding);
    if (header.phase != .ready) return error.SchemaValidationNotReady;
    try requireCurrent(binding, txn);
    return header;
}

fn requireSuccessor(a: A, txn: anytype, binding: Binding, installed: Installation) !void {
    try requireNamespace(binding, txn);
    if (!eq(binding.owner, try activation.ownership(txn)) or
        !eq(binding.after_schema, try digestAt(txn, schema.schema_key)) or
        !eq(binding.after_public, try digestAt(txn, public_key)) or
        !eq(binding.after_catalog, try digestAt(txn, integrity.key)) or
        !eq(installed.physical_catalog, try digestAt(txn, physical.head_key)) or
        !eq(installed.activation, try digestAt(txn, activation.key))) return error.SchemaValidationJobChanged;
    const catalog = try @import("table_catalog.zig").Catalog.decode((try optional(txn, @import("table_catalog.zig").key)) orelse return error.SchemaValidationJobChanged);
    if (catalog.storage_mode != .relational or catalog.active_schema_version != binding.after_version) return error.SchemaValidationJobChanged;
    // The old frames remain encoded with their original schema. Installation
    // must retain both native/public histories, not only the active pointers.
    for ([_]u32{ binding.before_version, binding.after_version }, [_]Digest{ binding.before_schema, binding.after_schema }, [_]Digest{ binding.before_public, binding.after_public }) |version, native_hash, public_hash| {
        const native_key = try schema.schemaVersionKeyAlloc(a, version);
        defer a.free(native_key);
        const public_version = try api.versionedSchemaKeyAlloc(a, version);
        defer a.free(public_version);
        if (!eq(native_hash, try digestAt(txn, native_key)) or !eq(public_hash, try digestAt(txn, public_version))) return error.SchemaValidationJobChanged;
    }
}

/// Called only after schema/catalog/activation writes have been staged by
/// the exact prepared installer, with its ready predecessor checked before
/// those writes. The containing transaction must also stage replay/outbox.
/// No fence release, signature, public RPC or coordinator ACK occurs here.
pub fn stageInstalled(a: A, txn: anytype, binding: Binding, ready_digest: Digest, decision: Digest, physical_catalog: Digest, entry: ?receipts.OrderedApplyReceipt) !Header {
    var header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    try requireCommitted(txn, header);
    if (!std.meta.eql(header.binding, binding) or header.phase != .ready or
        !eq(try header.digest(), ready_digest) or std.mem.allEqual(u8, &decision, 0)) return error.SchemaValidationJobChanged;
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    // Compute from the staged activation bytes; never promote independently
    // incomplete coverage merely because nullability validation completed.
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    const installed: Installation = .{ .decision = decision, .ready_digest = ready_digest, .ready_position = header.updated, .ready_last_effect = header.last_effect, .physical_catalog = physical_catalog, .activation = try digestAt(txn, activation.key), .position = position };
    try requireSuccessor(a, txn, binding, installed);
    header.phase = .installed;
    header.installation = installed;
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    header.last_effect = hash("schema-validation-install/v1");
    try save(a, txn, header);
    return header;
}

/// Read-only replay/recovery check of a committed local installation. This
/// does not authenticate a copied root or sign a distributed owner receipt.
pub fn requireInstalled(a: A, txn: anytype, binding: Binding, decision: Digest) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding) or header.phase != .installed or
        !eq(header.installation.?.decision, decision)) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    try requireSuccessor(a, txn, binding, header.installation.?);
    return header;
}

/// Durable progress observation for trusted owner retry/replay. No signature
/// or physical-root adoption authority is obtained from this local record.
pub fn observe(a: A, txn: anytype, binding: Binding) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding)) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    if (header.phase == .installed) return requireInstalled(a, txn, binding, header.installation.?.decision);
    if (header.phase == .canceled) {
        const done = (try topology.completed(txn)) orelse return error.SchemaValidationJobChanged;
        if (done.admission_epoch < binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
        return header;
    }
    return workHeader(txn, binding);
}

/// Read-only catalog preservation, never descriptor/publication admission.
/// Startup may use this before index runtimes exist. Only the actual durable
/// fence, namespace, phase cuts and catalog facts can suppress reconciliation.
pub fn protectedOwnerCatalog(a: A, txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace) !?Header {
    const fence = (try topology.current(txn)) orelse return null;
    if (fence.role != .schema_validation) return null;
    if (!fence.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
    const stored = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    return try observe(a, txn, stored.binding);
}

pub fn stageBegin(a: A, txn: anytype, binding: Binding, candidate: *const Candidate, entry: ?receipts.OrderedApplyReceipt) !Header {
    try binding.validate();
    try requireCandidate(binding, candidate);
    try requireCurrent(binding, txn);
    if (try load(txn)) |prior| {
        try requireCommitted(txn, prior);
        if (std.meta.eql(prior.binding, binding)) {
            if (prior.phase == .canceled) return error.SchemaValidationCanceled;
            return workHeader(txn, binding);
        }
        if (prior.phase != .canceled or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn) != null) return error.IntegrityTopologyBusy;
    const position = try nextPosition(txn, binding, entry);
    const header: Header = .{ .binding = binding, .phase = .fencing, .admitted = position, .updated = position, .cursor_digest = hash(""), .chain = hash(candidate.inputs), .last_effect = hash("schema-validation-begin/v1") };
    try txn.put(inputs_key, candidate.inputs);
    try txn.put(cursor_key, "");
    try save(a, txn, header);
    try topology.stageSchemaValidationBegin(txn, binding.fence);
    return header;
}
pub fn stageFrozen(a: A, txn: anytype, manager: *transactions.TxnManager, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !Header {
    var header = try workHeader(txn, binding);
    try requireCurrent(binding, txn);
    try topology.requireDrained(txn, manager, binding.fence);
    if (header.frozen != null) return header;
    if (header.phase != .fencing) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    header.frozen = position;
    header.updated = position;
    header.phase = .scanning;
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    return header;
}

/// Bounded transport command. Cursor bytes remain binary rather than passing
/// through JSON/UTF-8. Decoding is structural only: replay must independently
/// execute the prefix before any result is eligible for transactional apply.
pub const PageCommand = contract.PageCommand;

/// Trusted independently executed read preparation. Encoded page claims alone
/// are never accepted by stagePage; transport receivers must use replay.
pub const PreparedPage = struct {
    page: rows.NullabilityPage,
    expected: Header,
    pub fn deinit(self: *PreparedPage) void {
        self.page.deinit();
        self.* = undefined;
    }
    pub fn prepare(a: A, core: anytype, binding: Binding, budget: rows.Budget, control: projection.Control) !PreparedPage {
        return prepareMode(a, core, binding, budget, control, null);
    }
    /// Own the result before releasing the decoded command's backing bytes.
    /// No wall-time limit participates in the replicated stopping decision.
    pub fn replay(a: A, core: anytype, command_value: PageCommand, control: projection.Control) !PreparedPage {
        try command_value.validate();
        return prepareMode(a, core, command_value.binding, .{}, control, command_value);
    }
    /// The cursor borrows this prepared page. encode owns its output, allowing
    /// transport/recovery to discard the original page before replay.
    pub fn command(self: *const PreparedPage) !PageCommand {
        return .{ .binding = self.expected.binding, .expected = try self.expected.digest(), .boundary = .{ .records_examined = self.page.page.records_examined, .rows_examined = self.page.rows_examined, .violation_column = self.page.violation_column, .after = self.page.after, .more = self.page.page.more } };
    }
    fn prepareMode(a: A, core: anytype, binding: Binding, budget: rows.Budget, control: projection.Control, replay_command: ?PageCommand) !PreparedPage {
        try control.ensureActive();
        var reader = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            break :blk try rows.Reader.open(a, core.store, view, null, .{}, 0);
        };
        defer reader.deinit();
        const header = try workHeader(&reader.read, binding);
        try requireCurrent(binding, &reader.read);
        if (header.phase != .scanning) return error.SchemaValidationJobChanged;
        if (replay_command) |expected| if (!eq(try header.digest(), expected.expected)) return error.SchemaValidationJobChanged;
        const raw_cursor = (try optional(&reader.read, cursor_key)) orelse return error.InvalidSchemaValidationJob;
        if (raw_cursor.len > max_cursor or !eq(hash(raw_cursor), header.cursor_digest)) return error.InvalidSchemaValidationJob;
        const cursor = try a.dupe(u8, raw_cursor);
        defer a.free(cursor);
        var candidate = try Candidate.load(a, &reader.read, binding, control);
        defer candidate.deinit();
        try reader.resumeNullabilityScan(&candidate.plan, .{ .projection_identity = binding.projection_identity, .after = cursor }, control);
        const page = if (replay_command) |expected|
            try reader.nextNullabilityReplayPage(a, null, expected.boundary, &candidate.plan, control)
        else
            try reader.nextNullabilityPage(a, null, budget, &candidate.plan, control);
        return .{ .page = page, .expected = header };
    }
    fn effect(self: *const PreparedPage) !Digest {
        return (try self.command()).effect();
    }
};

pub fn stagePage(a: A, txn: anytype, manager: *transactions.TxnManager, prepared: *const PreparedPage, entry: ?receipts.OrderedApplyReceipt) !Header {
    const binding = prepared.expected.binding;
    var header = try workHeader(txn, binding);
    try topology.requireDrained(txn, manager, binding.fence);
    const effect = try prepared.effect();
    if (eq(header.last_effect, effect)) return header;
    if (header.phase != .scanning or !eq(try header.digest(), try prepared.expected.digest()) or
        prepared.page.rows_examined > prepared.page.page.records_examined or
        (prepared.page.page.more and prepared.page.page.records_examined == 0) or prepared.page.after.len > max_cursor)
        return error.SchemaValidationJobChanged;
    header.records = try std.math.add(u64, header.records, prepared.page.page.records_examined);
    header.rows_scanned = try std.math.add(u64, header.rows_scanned, prepared.page.rows_examined);
    header.revision = try std.math.add(u64, header.revision, 1);
    header.cursor_digest = hash(prepared.page.after);
    header.failure_column = if (prepared.page.violation_column) |column| std.math.cast(u16, column) orelse return error.InvalidSchemaValidationJob else null;
    header.phase = if (header.failure_column != null) .failed else if (prepared.page.page.more) .scanning else .ready;
    // The terminal validation cut rechecks the complete immutable metadata
    // once. Intermediate pages use the fixed fence/epoch guards above.
    if (header.phase == .ready) try requireCurrent(binding, txn);
    var chain = std.crypto.hash.Blake3.init(.{});
    chain.update(&header.chain);
    chain.update(&effect);
    chain.final(&header.chain);
    header.last_effect = effect;
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    header.updated = position;
    try txn.put(cursor_key, prepared.page.after);
    try save(a, txn, header);
    return header;
}

/// Consumes the admission epoch even when begin never arrived. Old schema,
/// rows, indexes and claims are unchanged. A dedicated terminal intent gates
/// release; generic topology cancellation stays unavailable.
pub fn stageCancel(a: A, txn: anytype, manager: *transactions.TxnManager, binding: Binding, entry: ?receipts.OrderedApplyReceipt) !Header {
    try binding.validate();
    try requireNamespace(binding, txn);
    const previous = try load(txn);
    if (previous) |prior| {
        // Cancellation is terminal progress, not permission to repair an
        // unproven restored/future cut by overwriting it with a local clock.
        try requireCommitted(txn, prior);
        if (prior.phase == .installed) return error.SchemaValidationPublicationRequired;
        if (std.meta.eql(prior.binding, binding) and prior.phase == .canceled) {
            return prior;
        }
        if (!std.meta.eql(prior.binding, binding) and (prior.phase != .canceled or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch))
            return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn)) |actual| if (!actual.eql(binding.fence)) return error.IntegrityTopologyBusy;
    if (try topology.completed(txn)) |done| if (done.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    if (previous) |prior| try precedes(prior.updated, position);
    var header: Header = if (previous != null and std.meta.eql(previous.?.binding, binding)) previous.? else .{ .binding = binding, .phase = .canceling, .admitted = null, .updated = position, .cursor_digest = hash(""), .chain = hash("schema-validation-unvisited/v1"), .last_effect = hash("schema-validation-cancel/v1") };
    header.phase = .canceling;
    header.failure_column = null;
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    try topology.stageSchemaValidationCancellation(txn, manager, binding.fence);
    header.phase = .canceled;
    header.last_effect = hash("schema-validation-cancel/v1");
    try save(a, txn, header);
    return header;
}
