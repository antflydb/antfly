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
const foreign_validation = @import("foreign_key_validation.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const topology = @import("relational_integrity_topology.zig");
const positions = @import("receipt_position.zig");
const authority = @import("../source_authority.zig");
const integrity = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const physical = @import("relational_index_catalog.zig");
const rows = @import("relational_rows.zig");
const transactions = @import("../transactions.zig");
const debt = transactions.topology_debt;
const receipts = @import("apply_receipts.zig");
const Digest = [32]u8;
pub const family_prefix = "\x00\x00__metadata__:schema_validation_job:";
pub const key = family_prefix ++ "v1";
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
    purpose: contract.Purpose = .not_null,
    foreign_source: ?foreign_validation.ChildPlan = null,
    foreign_parent: ?foreign_validation.ParentPlan = null,

    pub fn projectionCount(self: *const Candidate) usize {
        if (self.foreign_source) |source| return source.rules.len;
        if (self.foreign_parent) |parent| return parent.rules.len;
        return self.plan.names.len;
    }
    pub fn tableName(self: *const Candidate) ?[]const u8 {
        if (self.foreign_source) |source| return source.table_name;
        if (self.foreign_parent) |parent| return parent.rules[0].parent_table;
        return null;
    }
    pub fn deinit(self: *Candidate) void {
        if (self.foreign_source) |*source| source.deinit();
        if (self.foreign_parent) |*parent| parent.deinit();
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
        const inputs = try contract.encodeInputs(a, components);
        errdefer a.free(inputs);
        try control.ensureActive();
        return .{ .alloc = a, .inputs = inputs, .native_before = native_before, .native_after = native_after, .catalog_after = catalog_after, .plan = plan, .before_version = before.runtime_schema.version, .after_version = after.runtime_schema.version };
    }
    /// Exact child-side preparation. Parent descriptors bind tuple encoding;
    /// ordinary 2PC attachments establish parent validity, while metadata
    /// leases retain the selected target definitions through publication.
    pub fn initForeignKeySource(a: A, table_id: u64, before_json: []const u8, after_json: []const u8, predecessor: []const u8, config: foreign_validation.SourceConfig, control: projection.Control) !Candidate {
        try control.ensureActive();
        try config.validate();
        var counter = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(config, .{}, &counter.writer);
        if (counter.fullCount() > max_names) return error.InvalidSchemaValidationJob;
        const raw_config = try std.json.Stringify.valueAlloc(a, config, .{});
        defer a.free(raw_config);
        var temporary = std.heap.ArenaAllocator.init(a);
        defer temporary.deinit();
        const scratch = temporary.allocator();
        const names = try scratch.alloc([]const u8, config.parents.len);
        for (config.parents, names) |scope, *name| name.* = scope.constraint_name;
        var prepared_catalog = try @import("../../schema/foreign_key_validation.zig").prepareCatalogOwned(a, table_id, before_json, after_json, predecessor, names);
        defer prepared_catalog.deinit();
        const before = &prepared_catalog.before;
        const after = &prepared_catalog.after;
        if (before.runtime_schema.external_base_source != null or after.runtime_schema.external_base_source != null) return error.InvalidSchemaValidationJob;
        const previous = prepared_catalog.catalog.catalog;
        const native_before = try schema.serializeSchema(a, before.runtime_schema);
        errdefer a.free(native_before);
        const native_after = try schema.serializeSchema(a, after.runtime_schema);
        errdefer a.free(native_after);
        const definitions = try before.public_schema.relationalForeignKeyDefinitions(scratch);
        const uniques = try before.public_schema.relationalUniqueDefinitions(scratch);
        const rules = try scratch.alloc(foreign_validation.Rule, config.parents.len);
        for (config.parents, rules) |scope, *rule| {
            try control.ensureActive();
            const definition = for (definitions) |fk| {
                if (std.mem.eql(u8, fk.name, scope.constraint_name)) break fk;
            } else return error.InvalidSchemaValidationJob;
            if (!definition.not_valid or !std.mem.eql(u8, definition.parent_table, scope.declared_parent)) return error.InvalidSchemaValidationJob;
            const generation = previous.find(.foreign_key, definition.name) orelse return error.IntegrityCatalogChanged;
            if (scope.parent_table_id == table_id) {
                const target = try @import("../../schema/relational_foreign_key_target.zig").resolveUnique(uniques, definition.parent_columns);
                const unique = previous.find(.unique, target.name) orelse return error.IntegrityCatalogChanged;
                if (!std.mem.eql(u8, scope.parent_table, config.table_name) or scope.parent_schema_version != before.runtime_schema.version or
                    !std.mem.eql(u8, scope.parent_unique, target.name) or !eq(scope.parent_schema, hash(native_before)) or
                    !eq(scope.parent_public, hash(before_json)) or !eq(scope.parent_catalog, hash(predecessor)) or
                    !std.mem.eql(u8, &scope.parent_generation, &unique.generation)) return error.SchemaValidationJobChanged;
            }
            rule.* = .{ .name = definition.name, .columns = definition.child_columns, .generation = generation.generation, .parent_generation = scope.parent_generation, .match = definition.match };
        }
        var source = try foreign_validation.ChildPlan.init(a, config.table_name, before.runtime_schema, rules);
        errdefer source.deinit();
        var plan = try projection.Plan.init(a, before.runtime_schema, source.view.physicalLayout(), source.names, control);
        errdefer plan.deinit();
        plan.identity = source.identity;
        const inputs = try contract.encodeInputs(a, .{ before_json, after_json, predecessor, raw_config });
        errdefer a.free(inputs);
        const catalog_after = try a.dupe(u8, prepared_catalog.catalog.value);
        errdefer a.free(catalog_after);
        try control.ensureActive();
        return .{ .alloc = a, .inputs = inputs, .native_before = native_before, .native_after = native_after, .catalog_after = catalog_after, .plan = plan, .before_version = before.runtime_schema.version, .after_version = after.runtime_schema.version, .purpose = .foreign_key_source, .foreign_source = source };
    }
    /// Parent preparation is schema-neutral. Every selected UNIQUE is resolved
    /// from this owner's actual declaration and exact predecessor catalog. Child
    /// cuts are bound into the immutable input, but grant no source-freeze proof.
    pub fn initForeignKeyParent(a: A, table_id: u64, public_json: []const u8, predecessor: []const u8, config: foreign_validation.ParentConfig, control: projection.Control) !Candidate {
        try control.ensureActive();
        try config.validate();
        if (table_id == 0 or public_json.len == 0 or public_json.len > max_schema or predecessor.len == 0 or predecessor.len > integrity.max_catalog_bytes) return error.InvalidSchemaValidationJob;
        var counter = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(config, .{}, &counter.writer);
        if (counter.fullCount() > max_names) return error.InvalidSchemaValidationJob;
        const raw_config = try std.json.Stringify.valueAlloc(a, config, .{});
        defer a.free(raw_config);
        var prepared = try api.PreparedSchemaDefinition.init(a, public_json);
        defer prepared.deinit(a);
        if (prepared.runtime_schema.storage_mode != .relational or prepared.runtime_schema.external_base_source != null) return error.InvalidSchemaValidationJob;
        var installed_catalog = try integrity.decode(a, predecessor);
        defer installed_catalog.deinit();
        const coverage: compiler.CatalogCoverage = if (std.mem.allEqual(u8, &installed_catalog.checks_digest, 0)) .legacy else .checks_v1;
        var previous = try compiler.compileCatalogWithCoverage(a, prepared.public_schema, table_id, predecessor, coverage);
        defer previous.deinit();
        if (!std.mem.eql(u8, previous.value, predecessor)) return error.IntegrityCatalogChanged;
        var temporary = std.heap.ArenaAllocator.init(a);
        defer temporary.deinit();
        const scratch = temporary.allocator();
        const uniques = try prepared.public_schema.relationalUniqueDefinitions(scratch);
        const rules = try scratch.alloc(foreign_validation.ParentRule, config.children.len);
        var fields: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (config.children, rules) |scope, *rule| {
            try control.ensureActive();
            // Self references require the dual source/parent job. A passive
            // parent job alone cannot supply the missing frozen child cut.
            if (scope.child_table_id == table_id or std.mem.eql(u8, scope.child_table, config.table_name)) return error.InvalidSchemaValidationJob;
            const target = try @import("../../schema/relational_foreign_key_target.zig").resolveUnique(uniques, scope.parent_columns);
            const binding = previous.catalog.find(.unique, target.name) orelse return error.IntegrityCatalogChanged;
            if (!std.mem.eql(u8, target.name, scope.parent_unique) or !std.mem.eql(u8, &binding.generation, &scope.parent_generation)) return error.SchemaValidationJobChanged;
            rule.* = .{ .child_table = scope.child_table, .constraint_name = scope.constraint_name, .constraint_generation = scope.constraint_generation, .parent_table = config.table_name, .parent_generation = binding.generation };
            for (scope.parent_columns) |name| try fields.put(scratch, name, {});
        }
        var parent = try foreign_validation.ParentPlan.init(a, rules);
        errdefer parent.deinit();
        var layout = try codec.PhysicalLayout.init(a, prepared.runtime_schema);
        defer layout.deinit();
        var plan = try projection.Plan.init(a, prepared.runtime_schema, &layout, fields.keys(), control);
        errdefer plan.deinit();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/fk-validation-parent-projection/v1");
        state.update(raw_config);
        state.final(&plan.identity);
        const native_before = try schema.serializeSchema(a, prepared.runtime_schema);
        errdefer a.free(native_before);
        const native_after = try a.dupe(u8, native_before);
        errdefer a.free(native_after);
        const catalog_after = try a.dupe(u8, predecessor);
        errdefer a.free(catalog_after);
        const inputs = try contract.encodeInputs(a, .{ public_json, public_json, predecessor, raw_config });
        errdefer a.free(inputs);
        try control.ensureActive();
        return .{ .alloc = a, .inputs = inputs, .native_before = native_before, .native_after = native_after, .catalog_after = catalog_after, .plan = plan, .before_version = prepared.runtime_schema.version, .after_version = prepared.runtime_schema.version, .purpose = .foreign_key_parent, .foreign_parent = parent };
    }
    pub const parts = contract.inputParts;
    pub fn load(a: A, txn: anytype, binding: Binding, control: projection.Control) !Candidate {
        const raw = (try optional(txn, inputs_key)) orelse return error.SchemaValidationJobRequired;
        return fromInputs(a, raw, binding, control);
    }
    /// Terminal controls remove local ASVI bytes. Recovery may use the exact
    /// immutable inputs retained by metadata, never caller-asserted columns.
    pub fn fromInputs(a: A, raw: []const u8, binding: Binding, control: projection.Control) !Candidate {
        if (!eq(hash(raw), binding.inputs)) return error.SchemaValidationJobChanged;
        var result = try fromPreparationInputs(a, raw, binding.fence.namespace.table_id, binding.purpose, control);
        errdefer result.deinit();
        try requireCandidate(binding, &result);
        return result;
    }
    /// Read-only construction before the final binding exists. The native
    /// owner captures its own current catalog/root and signs the resulting cut.
    pub fn fromPreparationInputs(a: A, raw: []const u8, table_id: u64, purpose: contract.Purpose, control: projection.Control) !Candidate {
        const view = try parts(raw);
        var result: Candidate = if (purpose == .foreign_key_source) block: {
            var config = try std.json.parseFromSlice(foreign_validation.SourceConfig, a, view[3], .{ .ignore_unknown_fields = false });
            defer config.deinit();
            break :block try initForeignKeySource(a, table_id, view[0], view[1], view[2], config.value, control);
        } else if (purpose == .foreign_key_parent) block: {
            if (!std.mem.eql(u8, view[0], view[1])) return error.InvalidSchemaValidationJob;
            var config = try std.json.parseFromSlice(foreign_validation.ParentConfig, a, view[3], .{ .ignore_unknown_fields = false });
            defer config.deinit();
            break :block try initForeignKeyParent(a, table_id, view[0], view[2], config.value, control);
        } else if (purpose == .not_null) block: {
            var names = try std.json.parseFromSlice([]const []const u8, a, view[3], .{});
            defer names.deinit();
            break :block try init(a, table_id, view[0], view[1], view[2], names.value, control);
        } else return error.InvalidSchemaValidationJob;
        errdefer result.deinit();
        if (!std.mem.eql(u8, raw, result.inputs)) return error.InvalidSchemaValidationJob;
        return result;
    }
};

pub const Binding = contract.Binding;

pub fn prepareBinding(txn: anytype, candidate: *const Candidate, id: [16]u8, plan_digest: Digest, fence: topology.Fence, kind: authority.Kind) !Binding {
    const parts = try Candidate.parts(candidate.inputs);
    const result: Binding = .{
        .purpose = candidate.purpose,
        .id = id,
        .plan_digest = plan_digest,
        .fence = fence,
        .authority = kind,
        .owner = try activation.ownership(txn),
        .before_version = candidate.before_version,
        .after_version = candidate.after_version,
        .columns = @intCast(candidate.projectionCount()),
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
    if (self.purpose != candidate.purpose) return error.InvalidSchemaValidationJob;
    const parts = try Candidate.parts(candidate.inputs);
    if (self.before_version != candidate.before_version or self.after_version != candidate.after_version or
        self.columns != candidate.projectionCount() or !eq(self.projection_identity, candidate.plan.identity) or
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

/// Small durable guards only. A rejection is a command result, never a catch-all
/// interpretation of preparation/scan/storage errors. Namespace, authority,
/// corrupt records and future receipt positions remain fatal/recovery errors.
pub fn guardRejection(txn: anytype, command: contract.Command) !?@import("relational_schema_apply_outcome.zig").Reason {
    const binding = command.binding;
    try requireNamespace(binding, txn);
    if (binding.purpose == .foreign_key_parent or binding.purpose == .foreign_key_dual) return .publication_required;
    const stored = try load(txn);
    var installed_duplicate = false;
    var active_binding = false;
    if (stored) |header| {
        try requireCommitted(txn, header);
        if (!std.meta.eql(header.binding, binding)) {
            if (header.binding.fence.admission_epoch >= binding.fence.admission_epoch or
                (header.phase != .released and header.phase != .canceled) or
                (command.action != .begin and command.action != .cancel)) return .job_changed;
        } else {
            if (header.phase == .canceled and command.action != .cancel) return .canceled;
            installed_duplicate = header.phase == .installed or header.phase == .released;
            active_binding = !installed_duplicate and header.phase != .canceled;
            if (command.action == .cancel and installed_duplicate) return .publication_required;
            if (command.action == .release and !installed_duplicate) return .job_changed;
            if (command.action == .install and header.phase != .ready and !installed_duplicate) return .job_changed;
            switch (command.action) {
                .reconcile_transactions => {
                    if (eq(header.last_effect, contract.transactionDebtEffect(command.payload[0..32].*))) return null;
                    if (header.phase != .fencing) return .job_changed;
                    const directory = try debt.require(txn, try debt.Scope.fromBinding(binding));
                    if (directory.phase == .ready or directory.phase == .retired or
                        !eq(try directory.digest(), command.payload[0..32].*)) return .predecessor_changed;
                },
                .begin => if (header.admitted != null) return null,
                .freeze => if (header.frozen != null) return null,
                .cancel => if (header.phase == .canceled) return null,
                .install => {
                    if (installed_duplicate) {
                        if (!eq(header.installation.?.decision, command.payload[0..32].*) or
                            !eq(header.installation.?.ready_digest, command.payload[32..64].*)) return .job_changed;
                        return null;
                    }
                    if (!eq(try header.digest(), command.payload[32..64].*)) return .predecessor_changed;
                },
                .release => {
                    if (!eq(header.installation.?.decision, command.payload[0..32].*)) return .job_changed;
                    const expected = if (header.release) |released| released.installed_digest else try header.digest();
                    if (!eq(expected, command.payload[32..64].*)) return .predecessor_changed;
                    if (header.release) |released| {
                        if (!eq(released.publication, command.payload[64..96].*)) return .job_changed;
                        return null;
                    }
                },
                .page => {},
            }
            if (command.action == .page) {
                const page = try contract.PageCommand.decode(command.payload);
                if (eq(header.last_effect, try page.effect())) return null else {
                    if (header.phase != .scanning or !eq(try header.digest(), page.expected)) return .predecessor_changed;
                }
            }
        }
    } else if (command.action != .begin and command.action != .cancel) return .job_required;
    if (try topology.current(txn)) |fence| {
        if (!fence.namespace.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        if (!fence.eql(binding.fence)) return .topology_changed;
    } else if (command.action != .begin and command.action != .cancel) return .topology_changed;
    // Cancellation can consume an abandoned epoch before BEGIN arrives, even
    // if the public schema has since changed. It never installs that schema.
    if (command.action == .cancel and !active_binding) return null;
    if (!installed_duplicate) {
        const catalog_mod = @import("table_catalog.zig");
        const catalog = try catalog_mod.Catalog.decode((try optional(txn, catalog_mod.key)) orelse return error.SchemaValidationRuntimeRecoveryRequired);
        if (catalog.storage_mode != .relational or catalog.active_schema_version != binding.before_version)
            return if (active_binding) error.SchemaValidationRuntimeRecoveryRequired else .schema_changed;
        const physical_bytes = try optional(txn, physical.head_key);
        if (physical_bytes) |bytes| _ = try physical.Head.decode(bytes);
        if (!eq(binding.physical_catalog, hash(physical_bytes orelse "")))
            return if (active_binding) error.SchemaValidationRuntimeRecoveryRequired else .physical_catalog_changed;
        if (!eq(binding.owner, try activation.ownership(txn)))
            return if (active_binding) error.SchemaValidationRuntimeRecoveryRequired else .owner_changed;
    }
    if (command.action == .install and !installed_duplicate and
        try transactions.TxnManager.hasQualifiedTopologyDebt(txn, try debt.Scope.fromBinding(binding))) return .transactions_pending;
    return null;
}

pub const Phase = contract.Phase;
pub const Installation = contract.Installation;
pub const Header = contract.Header;

const precedesOrEqual = contract.precedesOrEqual;
const precedes = contract.precedes;
pub fn load(txn: anytype) !?Header {
    return if (try optional(txn, key)) |bytes| try Header.decode(bytes) else null;
}

pub fn observeTransactionDebt(txn: anytype, binding: Binding) !?contract.TransactionDebtObservation {
    const header = (try load(txn)) orelse return null;
    if (!std.meta.eql(header.binding, binding)) return error.SchemaValidationJobChanged;
    // A never-visited cancellation has no admitted writer generation.
    if (header.admitted == null) return null;
    const directory = (try debt.load(txn)) orelse return error.SchemaValidationRuntimeRecoveryRequired;
    if (!directory.scope.eql(try debt.Scope.fromBinding(binding)) or
        ((directory.phase == .retired) != (header.phase == .released or header.phase == .canceled))) return error.SchemaValidationJobChanged;
    const result: contract.TransactionDebtObservation = .{
        .revision = directory.revision,
        .debt = directory.debt,
        .ready = directory.phase == .ready,
        .retired = directory.phase == .retired,
        .expected = try directory.digest(),
    };
    try result.validate();
    return result;
}

/// Imported semantic scan progress is an inert source checkpoint. Verify its
/// canonical definition and pair, but grant neither current-root coverage nor
/// metadata adoption. A replacement native root restarts it from zero in 2PC.
fn validateTransactionSnapshot(a: A, txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace, header: ?Header, control: @import("antfly_cancellation").CancellationToken) !bool {
    const raw = (try optional(txn, activation.validation_key)) orelse {
        if (try optional(txn, activation.validation_inputs_key) != null) return error.InvalidSchemaValidationJob;
        return false;
    };
    if (raw.len > activation.ValidationRecord.max_wire_bytes) return error.InvalidSchemaValidationJob;
    const owned = try a.dupe(u8, raw);
    defer a.free(owned);
    const record = try activation.ValidationRecord.decode(owned);
    const inputs = (try optional(txn, activation.validation_inputs_key)) orelse return error.InvalidSchemaValidationJob;
    if (inputs.len > contract.max_input_bytes or !eq(hash(inputs), record.binding.inputs)) return error.InvalidSchemaValidationJob;
    const Check = struct {
        fn active(context: ?*anyopaque) anyerror!void {
            const token: *const @import("antfly_cancellation").CancellationToken = @ptrCast(@alignCast(context.?));
            try token.check();
        }
    };
    var candidate = try Candidate.fromInputs(a, inputs, record.binding, .{ .context = @constCast(&control), .checkpoint = Check.active });
    defer candidate.deinit();
    var predecessor = try integrity.decode(a, (try contract.inputParts(candidate.inputs))[2]);
    defer predecessor.deinit();
    if (!record.progress.matches(predecessor, record.binding.owner)) return error.InvalidSchemaValidationJob;
    try control.check();
    if (header) |value| {
        if (value.phase != .canceled and value.phase != .released) {
            if (value.transaction_coverage) |coverage| {
                if (!std.meta.eql(value.binding, record.binding) or !eq(coverage.record, hash(owned)) or coverage.physical_root != record.physical_root or
                    record.progress.state == .validating or
                    ((value.phase == .ready or value.phase == .installed) and record.progress.state != .enforced) or
                    (value.phase == .failed and record.progress.state != .invalid)) return error.SchemaValidationRuntimeRecoveryRequired;
            }
        }
    }
    if (record.progress.state == .invalid) return true;
    if (!record.binding.fence.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
    if (header) |value| {
        if (value.phase != .canceled and value.phase != .released) {
            if (value.transaction_coverage == null) return error.SchemaValidationRuntimeRecoveryRequired;
            if (value.installation != null) {
                try requireSuccessorDefinition(a, txn, record.binding);
                return true;
            }
        } else if (value.binding.fence.admission_epoch >= record.binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    }
    try activation.requireValidationDefinition(txn, record.binding);
    if (!eq(try digestAt(txn, public_key), record.binding.before_public)) return error.SchemaValidationJobChanged;
    return true;
}

/// Verify local progress evidence in the same immutable read as the imported
/// rows and ordered marker. This grants neither physical-root adoption nor a
/// metadata ACK. Nonterminal foreign jobs need explicit owner adoption; old
/// terminal history remains inert in the destination's independent clock.
pub fn validateSnapshot(a: A, txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace, options: @import("../../spill_sort.zig").Options) !void {
    const control = options.cancellation orelse @import("antfly_cancellation").CancellationToken.none;
    try control.check();
    const observed = try load(txn);
    const has_transaction = try validateTransactionSnapshot(a, txn, namespace, observed, control);
    const header = observed orelse {
        if (try debt.hasPrefix(txn, debt.family_prefix)) return error.SchemaValidationRuntimeRecoveryRequired;
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(family_prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, family_prefix)) break;
            try control.check();
            if (!has_transaction or (!std.mem.eql(u8, row.key, activation.validation_key) and !std.mem.eql(u8, row.key, activation.validation_inputs_key))) return error.SchemaValidationRuntimeRecoveryRequired;
        }
        _ = try transactions.TxnManager.validateTopologyDebtSnapshot(a, txn, null, options);
        return;
    };
    if (header.binding.fence.namespace.eql(namespace)) {
        try requireCommitted(txn, header);
        if (header.phase == .canceled or header.phase == .released) try requireCompletion(txn, header.binding);
    } else if (header.phase != .released and header.phase != .canceled) return error.SchemaValidationRuntimeRecoveryRequired;
    _ = try observeTransactionDebt(txn, header.binding);
    if (header.binding.authority == .raft) {
        var positions_to_check: [5]?positions.Position = .{ header.admitted, header.frozen, header.updated, null, null };
        if (header.installation) |installed| {
            positions_to_check[3] = installed.ready_position;
            positions_to_check[4] = installed.position;
        }
        const outcomes = @import("relational_schema_apply_outcome.zig");
        for (positions_to_check) |maybe_position| if (maybe_position) |position| {
            try control.check();
            const stored = (try outcomes.read(txn, header.binding.fence.namespace, position.raft.index)) orelse
                return error.SchemaValidationRuntimeRecoveryRequired;
            if (stored.entry.term != position.raft.term or stored.reason != .accepted) return error.CorruptSchemaApplyOutcome;
        };
    }
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(family_prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, family_prefix)) break;
            try control.check();
            if (!std.mem.eql(u8, row.key, key) and !std.mem.eql(u8, row.key, inputs_key) and !std.mem.eql(u8, row.key, cursor_key) and
                !(has_transaction and (std.mem.eql(u8, row.key, activation.validation_key) or std.mem.eql(u8, row.key, activation.validation_inputs_key)))) return error.InvalidSchemaValidationJob;
        }
    }
    // Namespace/cut/completion checks are fixed work. Reject those failures
    // before spending bounded scratch/IO on a potentially large old cohort.
    _ = try transactions.TxnManager.validateTopologyDebtSnapshot(a, txn, if (header.admitted != null) try debt.Scope.fromBinding(header.binding) else null, options);
}
fn save(a: A, txn: anytype, header: Header) !void {
    const bytes = try header.encode(a);
    defer a.free(bytes);
    try txn.put(key, bytes);
}
fn requireCommitted(txn: anytype, header: Header) !void {
    try precedesOrEqual(header.updated, try currentPosition(txn, header.binding));
}
fn currentPosition(txn: anytype, binding: Binding) !positions.Position {
    try requireNamespace(binding, txn);
    const actual: positions.Position = if (binding.authority == .native)
        .{ .native = .{ .namespace = binding.fence.namespace, .sequence = (try authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace))).sequence } }
    else blk: {
        const entry = (try receipts.readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
        break :blk .{ .raft = .{ .term = entry.term, .index = entry.index } };
    };
    try actual.validate();
    return actual;
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
    if (!header.binding.fence.eql(fence) or header.phase == .canceled or header.phase == .released) return error.SchemaValidationJobChanged;
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
    if (!std.meta.eql(header.binding, binding) or header.phase == .canceling or header.phase == .canceled or header.phase == .released) return error.SchemaValidationJobChanged;
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
    if (header.transaction_coverage) |coverage| _ = try requireTransactionCoverage(txn, binding, coverage.record);
    try requireCurrent(binding, txn);
    if (try transactions.TxnManager.hasQualifiedTopologyDebt(txn, try debt.Scope.fromBinding(binding))) return error.TransactionTopologyBusy;
    return header;
}

fn requireSuccessor(a: A, txn: anytype, binding: Binding, installed: Installation) !void {
    try requireSuccessorDefinition(a, txn, binding);
    if (!eq(installed.physical_catalog, try digestAt(txn, physical.head_key)) or
        !eq(installed.activation, try digestAt(txn, activation.key))) return error.SchemaValidationJobChanged;
}

/// Released tables accept normal writes and independent coverage progress;
/// the installation's old activation bytes are no longer a current-row cut.
/// Immutable definitions/history still bind the candidate's typed projection.
fn requireSuccessorDefinition(a: A, txn: anytype, binding: Binding) !void {
    try requireNamespace(binding, txn);
    if (!eq(binding.owner, try activation.ownership(txn)) or
        !eq(binding.after_schema, try digestAt(txn, schema.schema_key)) or
        !eq(binding.after_public, try digestAt(txn, public_key)) or
        !eq(binding.after_catalog, try digestAt(txn, integrity.key))) return error.SchemaValidationJobChanged;
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
    if (binding.purpose != .not_null and (binding.purpose != .foreign_key_source or header.transaction_coverage == null)) return error.ForeignKeyValidationRequired;
    if (header.transaction_coverage) |coverage| _ = try requireTransactionCoverage(txn, binding, coverage.record);
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
    const header = try requireInstalledCut(txn, binding, decision);
    try requireSuccessor(a, txn, binding, header.installation.?);
    return header;
}

/// The installed schema/history were verified during off-lock preparation and
/// cannot be changed through this still-active exclusive lifecycle fence.
/// Apply checks only bounded durable facts; never hash multi-MiB definitions
/// or allocate historical-schema keys in the serialized release transaction.
fn requireInstalledCut(txn: anytype, binding: Binding, decision: Digest) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding) or header.phase != .installed or
        !eq(header.installation.?.decision, decision)) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    const catalog = try @import("table_catalog.zig").Catalog.decode((try optional(txn, @import("table_catalog.zig").key)) orelse return error.SchemaValidationJobChanged);
    if (catalog.storage_mode != .relational or catalog.active_schema_version != binding.after_version or
        !eq(binding.owner, try activation.ownership(txn)) or
        !eq(header.installation.?.physical_catalog, try digestAt(txn, physical.head_key))) return error.SchemaValidationJobChanged;
    return header;
}

/// Trusted publication adapter only. A digest is not authority: the receiving
/// owner must independently authenticate the metadata all-installed decision
/// before invoking this effect. Replay/outbox joins the caller's SAME writer.
pub fn stageReleased(a: A, txn: anytype, manager: *transactions.TxnManager, binding: Binding, install_decision: Digest, installed_digest: Digest, publication: Digest, entry: ?receipts.OrderedApplyReceipt) !Header {
    if (std.mem.allEqual(u8, &publication, 0)) return error.InvalidSchemaOwnerControl;
    var header = try requireInstalledCut(txn, binding, install_decision);
    if (!eq(try header.digest(), installed_digest)) return error.SchemaValidationJobChanged;
    try topology.requireDrained(txn, manager, binding.fence);
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    header.release = .{ .publication = publication, .installed_digest = installed_digest, .installed_last_effect = header.last_effect };
    header.phase = .released;
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    header.last_effect = hash("schema-validation-release/v1");
    try save(a, txn, header);
    try topology.stageSchemaValidationRelease(txn, header);
    try debt.retire(txn, try debt.Scope.fromBinding(binding));
    // Terminal observation needs only its immutable installed predecessor.
    // Reclaim the potentially multi-MiB preparation/cursor in this transaction.
    try txn.delete(inputs_key);
    try txn.delete(cursor_key);
    if (header.transaction_coverage) |coverage| {
        _ = try requireTransactionCoverage(txn, binding, coverage.record);
        try txn.delete(activation.validation_key);
        try txn.delete(activation.validation_inputs_key);
    }
    return header;
}

/// Verify terminal completion, not today's row/schema bytes. User writes and
/// later legitimate schema epochs must not invalidate an earlier release ACK.
pub fn requireReleased(txn: anytype, expected: Header) !Header {
    try expected.validate();
    if (expected.phase != .released) return error.SchemaValidationPublicationRequired;
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header, expected)) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    try requireCompletion(txn, header.binding);
    return header;
}

fn requireCompletion(txn: anytype, binding: Binding) !void {
    const done = (try topology.completed(txn)) orelse return error.SchemaValidationJobChanged;
    if (done.admission_epoch < binding.fence.admission_epoch or
        (done.admission_epoch == binding.fence.admission_epoch and !done.eql(binding.fence))) return error.SchemaValidationJobChanged;
    // A copied/torn terminal header must not produce a release ACK while the
    // old fence still exists. A later independent lifecycle fence is history.
    if (try optional(txn, topology.fence_key)) |raw| {
        const live = try topology.Fence.decode(raw);
        if (live.admission_epoch <= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    }
}

/// Durable progress observation for trusted owner retry/replay. No signature
/// or physical-root adoption authority is obtained from this local record.
pub fn observe(a: A, txn: anytype, binding: Binding) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding)) return error.SchemaValidationJobChanged;
    try requireCommitted(txn, header);
    if (header.phase == .installed) return requireInstalled(a, txn, binding, header.installation.?.decision);
    if (header.phase == .released) return requireReleased(txn, header);
    if (header.phase == .canceled) {
        try requireCompletion(txn, binding);
        return header;
    }
    return workHeader(txn, binding);
}

/// Transactional recheck for an independently prepared owner control. The
/// immutable read preparation already verified full installed history. The
/// fence, owner, epoch and physical catalog prevent it changing before apply.
pub fn observeForApply(txn: anytype, binding: Binding) !Header {
    const header = (try load(txn)) orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(header.binding, binding)) return error.SchemaValidationJobChanged;
    if (header.phase == .installed) return requireInstalledCut(txn, binding, header.installation.?.decision);
    if (header.phase == .released) return requireReleased(txn, header);
    try requireCommitted(txn, header);
    if (header.phase == .canceled) {
        try requireCompletion(txn, binding);
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

/// Only the exact completed transaction record can enter publication. This
/// does not authenticate a physical root; the receiving DB checks that root
/// before preparing the control, while this helper checks the durable cut.
fn requireTransactionCoverage(txn: anytype, binding: Binding, expected: ?Digest) !activation.ValidationRecord {
    const record = try requireTerminalTransactionRecord(txn, binding, expected);
    if (record.progress.state != .enforced) return error.SchemaValidationNotReady;
    return record;
}
fn requireTerminalTransactionRecord(txn: anytype, binding: Binding, expected: ?Digest) !activation.ValidationRecord {
    const bytes = (try optional(txn, activation.validation_key)) orelse return error.SchemaValidationNotReady;
    if (expected) |value| if (!eq(hash(bytes), value)) return error.SchemaValidationJobChanged;
    const record = try activation.ValidationRecord.decode(bytes);
    if (!std.meta.eql(record.binding, binding)) return error.SchemaValidationJobChanged;
    if (record.progress.state == .validating) return error.SchemaValidationNotReady;
    return record;
}

pub fn stageBegin(a: A, txn: anytype, binding: Binding, candidate: *const Candidate, entry: ?receipts.OrderedApplyReceipt) !Header {
    // Parent preparation must never fall through to the source's write fence
    // and row scanner. Admission requires the complete frozen-source cohort.
    if (binding.purpose == .foreign_key_parent or binding.purpose == .foreign_key_dual) return error.ForeignKeyValidationRequired;
    try binding.validate();
    try requireCandidate(binding, candidate);
    try requireCurrent(binding, txn);
    if (try load(txn)) |prior| {
        try requireCommitted(txn, prior);
        if (prior.phase == .released or prior.phase == .canceled) try requireCompletion(txn, prior.binding);
        if (std.meta.eql(prior.binding, binding)) {
            if (prior.phase == .canceled) return error.SchemaValidationCanceled;
            return workHeader(txn, binding);
        }
        if ((prior.phase != .canceled and prior.phase != .released) or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn) != null) return error.IntegrityTopologyBusy;
    const position = try nextPosition(txn, binding, entry);
    var header: Header = .{ .binding = binding, .phase = .fencing, .admitted = position, .updated = position, .cursor_digest = hash(""), .chain = hash(candidate.inputs), .last_effect = hash("schema-validation-begin/v1"), .reference_coverage = if (binding.purpose == .not_null) null else .{} };
    if (binding.purpose == .foreign_key_source) {
        const record = try requireTerminalTransactionRecord(txn, binding, null);
        header.reference_coverage = null;
        header.transaction_coverage = .{ .record = try digestAt(txn, activation.validation_key), .physical_root = record.physical_root };
    }
    try txn.put(inputs_key, candidate.inputs);
    try txn.put(cursor_key, "");
    try save(a, txn, header);
    try topology.stageSchemaValidationBegin(txn, binding.fence);
    try transactions.TxnManager.stageTopologyDebtBegin(txn, try debt.Scope.fromBinding(binding));
    return header;
}

/// Independent durable control, never a sub-effect of a FREEZE that may fail
/// while later coordinator ACKs remain queued. Preparation owns bounded work;
/// apply compares the exact directory predecessor and advances the real clock.
pub fn stageTransactionDebtPage(a: A, txn: anytype, binding: Binding, page: *const debt.Page, entry: ?receipts.OrderedApplyReceipt) !Header {
    var header = try workHeader(txn, binding);
    const effect = contract.transactionDebtEffect(page.expected);
    if (eq(header.last_effect, effect)) return header;
    if (header.phase != .fencing) return error.SchemaValidationJobChanged;
    try requireCurrent(binding, txn);
    const scope = try debt.Scope.fromBinding(binding);
    if (!page.next.scope.eql(scope)) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    try precedes(header.updated, position);
    try page.stage(txn);
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    header.last_effect = effect;
    try save(a, txn, header);
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
    header.last_effect = hash("schema-validation-freeze/v1");
    if (header.transaction_coverage) |coverage| {
        const record = try requireTerminalTransactionRecord(txn, binding, coverage.record);
        // Every page joined guarded child observations, real parent effects,
        // and this exact progress CAS in 2PC. New writes enforce the FK and
        // validated references cannot revert to historical references. Drain
        // outstanding transactions before turning that cut into readiness.
        header.phase = if (record.progress.state == .enforced) .ready else .failed;
        header.failure_column = if (record.progress.state == .invalid) 0 else null;
        header.records = record.progress.rows_scanned;
        header.rows_scanned = record.progress.rows_scanned;
        header.cursor_digest = hash(record.progress.cursor);
        header.chain = coverage.record;
        header.last_effect = hash("schema-validation-transaction-freeze/v1");
        try txn.put(cursor_key, record.progress.cursor);
    }
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    return header;
}

/// Bounded transport command. Cursor bytes remain binary rather than passing
/// through JSON/UTF-8. Decoding is structural only: replay must independently
/// execute the prefix before any result is eligible for transactional apply.
pub const PageCommand = contract.PageCommand;

/// Pin schema and data under one short publication fence. Reader allocation,
/// range decoding and planning run after unlocking, for both validation and
/// recovery. openSnapshot consumes its transaction even when preparation fails.
fn openPhysicalReader(a: A, core: anytype) !rows.Reader {
    const captured = blk: {
        core.lockApplyShared();
        defer core.unlockApplyShared();
        var view = core.acquireSchemaView() orelse return error.RelationalTableRequired;
        errdefer view.release();
        break :blk .{ .view = view, .read = try core.store.beginReadTxn(), .authenticated = core.store.valuesAreAuthenticated() };
    };
    var view = captured.view;
    defer view.release();
    return rows.Reader.openSnapshot(a, captured.read, captured.authenticated, view, null, .{}, 0);
}

/// Physical row coverage only, NOT authority to adopt a root or publish a
/// schema. The recovery protocol must additionally qualify transaction debt,
/// source/root provenance and the exact metadata predecessor. A retained MVCC
/// read keeps page boundaries independent of concurrent compaction or writes.
pub const RowAudit = struct {
    pub const Coverage = @import("relational_schema_root_contract.zig").Coverage;
    pub const Summary = @import("relational_schema_root_contract.zig").RowSummary;
    /// Root-local authenticated continuation, never a remote progress claim.
    /// Its owner must verify the checkpoint signature before resuming it.
    pub const Progress = struct {
        alloc: A,
        summary: Summary,
        after: []u8,
        complete: bool,

        pub fn deinit(self: *@This()) void {
            self.alloc.free(self.after);
            self.* = undefined;
        }
        pub fn validate(self: *const @This()) !void {
            const value = self.summary;
            try value.validateProgress();
            try value.snapshot_cut.requireNamespace(value.header.binding.fence.namespace);
            try precedesOrEqual(value.header.updated, value.snapshot_cut);
            if (value.coverage != coverageFor(value.header) or value.rows > value.records or self.after.len > max_cursor or
                !eq(hash(self.after), value.cursor_digest) or ((value.records == 0) != (self.after.len == 0)) or
                (self.after.len != 0 and !@import("../internal_keys.zig").isRelationalRowKey(self.after))) return error.InvalidSchemaValidationJob;
            switch (value.coverage) {
                .none, .history => if (!self.complete or value.records != 0 or value.rows != 0) return error.InvalidSchemaValidationJob,
                .prefix => {
                    if (value.records > value.header.records or value.rows > value.header.rows_scanned or
                        self.complete != (value.records == value.header.records) or (value.header.phase == .failed and value.header.records == 0)) return error.InvalidSchemaValidationJob;
                    if (self.complete and (value.rows != value.header.rows_scanned or !eq(value.cursor_digest, value.header.cursor_digest))) return error.InvalidSchemaValidationJob;
                },
                .full => if (value.header.phase != .released) {
                    if (value.records > value.header.records or value.rows > value.header.rows_scanned or
                        (self.complete and (value.records != value.header.records or value.rows != value.header.rows_scanned or
                            !eq(value.cursor_digest, value.header.cursor_digest)))) return error.InvalidSchemaValidationJob;
                },
            }
        }
    };

    fn coverageFor(header: Header) Coverage {
        return switch (header.phase) {
            .fencing, .canceling, .canceled => .none,
            .scanning, .failed => .prefix,
            .ready, .installed => .full,
            .released => if (header.transaction_coverage != null) .history else .full,
        };
    }
    alloc: A,
    reader: rows.Reader,
    candidate: Candidate,
    header: Header,
    snapshot_cut: positions.Position,
    coverage: Coverage,
    records: u64 = 0,
    rows_examined: u64 = 0,
    reference_coverage: ?contract.ReferenceCoverage = null,
    cursor_digest: Digest,
    done: bool = false,
    invalid: bool = false,

    pub fn deinit(self: *@This()) void {
        self.candidate.deinit();
        self.reader.deinit();
        self.* = undefined;
    }

    pub fn init(a: A, core: anytype, binding: Binding, retained_inputs: []const u8, control: projection.Control) !@This() {
        if (binding.purpose == .foreign_key_parent or binding.purpose == .foreign_key_dual) return error.ForeignKeyValidationRequired;
        try control.ensureActive();
        var reader = try openPhysicalReader(a, core);
        errdefer reader.deinit();
        const header = try observe(a, &reader.read, binding);
        if (header.phase == .released) {
            try requireSuccessorDefinition(a, &reader.read, binding);
        } else if (header.installation == null) try requireCurrent(binding, &reader.read);
        if (try optional(&reader.read, inputs_key)) |raw| {
            if (!eq(hash(raw), binding.inputs)) return error.SchemaValidationJobChanged;
        } else if (header.phase != .released and header.phase != .canceled) return error.SchemaValidationJobRequired;
        var candidate = try Candidate.fromInputs(a, retained_inputs, binding, control);
        errdefer candidate.deinit();
        const coverage = coverageFor(header);
        // A fresh reader starts at the real beginning. Projection binding is
        // deferred until its first page or authenticated checkpoint resume.
        const snapshot_cut = try currentPosition(&reader.read, binding);
        var result: @This() = .{ .alloc = a, .reader = reader, .candidate = candidate, .header = header, .snapshot_cut = snapshot_cut, .coverage = coverage, .reference_coverage = if (binding.purpose == .not_null or coverage == .history) null else .{}, .cursor_digest = hash("") };
        if (coverage == .none or coverage == .history) result.done = true else if (coverage == .prefix and header.records == 0) {
            if (header.phase == .failed or header.rows_scanned != 0 or !eq(header.cursor_digest, hash(""))) return error.SchemaValidationRuntimeRecoveryRequired;
            result.done = true;
        }
        return result;
    }

    pub fn checkpoint(self: *const @This(), a: A) !Progress {
        if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
        const after = try a.dupe(u8, self.reader.after.items);
        errdefer a.free(after);
        const result: Progress = .{ .alloc = a, .summary = self.snapshotSummary(), .after = after, .complete = self.done };
        try result.validate();
        return result;
    }

    /// Caller must authenticate root-local bytes first. No transport endpoint
    /// may accept this structure from a client as evidence of scanned rows.
    pub fn resumeAuthenticatedCheckpoint(self: *@This(), progress: *const Progress, control: projection.Control) !void {
        try control.ensureActive();
        try progress.validate();
        if (self.invalid or self.records != 0 or self.reader.after.items.len != 0 or
            !std.meta.eql(self.header, progress.summary.header) or !std.meta.eql(self.snapshot_cut, progress.summary.snapshot_cut) or
            self.coverage != progress.summary.coverage) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.coverage == .history) {
            if (!progress.complete or !std.meta.eql(self.snapshotSummary(), progress.summary) or progress.after.len != 0)
                return error.SchemaValidationRuntimeRecoveryRequired;
            return;
        }
        if (self.candidate.foreign_source) |*source| {
            try self.reader.resumeForeignKeyValidationScan(source, .{ .projection_identity = self.header.binding.projection_identity, .after = progress.after }, control);
        } else try self.reader.resumeNullabilityScan(&self.candidate.plan, .{ .projection_identity = self.header.binding.projection_identity, .after = progress.after }, control);
        self.reference_coverage = progress.summary.reference_coverage;
        self.records = progress.summary.records;
        self.rows_examined = progress.summary.rows;
        self.cursor_digest = progress.summary.cursor_digest;
        self.done = progress.complete;
    }

    /// One bounded page, without an apply lock or JSON reconstruction. Callers
    /// may yield between calls; cancellation never consumes a partial page.
    pub fn step(self: *@This(), io: ?std.Io, budget: rows.Budget, control: projection.Control) !bool {
        try control.ensureActive();
        try budget.validate();
        if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.done) return true;
        var bounded = budget;
        if (self.coverage == .prefix) bounded.records = @intCast(@min(@as(u64, @intCast(budget.records)), self.header.records - self.records));
        var delta: ?contract.ReferenceCoverage = null;
        var page = if (self.candidate.foreign_source) |*source| block: {
            const value = try self.reader.nextForeignKeyValidationPage(self.alloc, io, bounded, source, control);
            delta = .{ .expected = value.witness };
            break :block value.scan;
        } else try self.reader.nextNullabilityPage(self.alloc, io, bounded, &self.candidate.plan, control);
        defer page.deinit();
        // A rejected page has advanced the retained reader. Never retry it as
        // an empty EOF page and accidentally qualify the previous counters.
        errdefer self.invalid = true;
        const records = try std.math.add(u64, self.records, page.page.records_examined);
        const examined = try std.math.add(u64, self.rows_examined, page.rows_examined);
        var reference_coverage = self.reference_coverage;
        if (delta) |value| {
            try reference_coverage.?.expected.combine(value.expected);
            try reference_coverage.?.observed.combine(value.observed);
        }
        if (self.header.phase != .released and (records > self.header.records or examined > self.header.rows_scanned)) return error.SchemaValidationRuntimeRecoveryRequired;
        const cursor = hash(page.after);
        const violation: ?u16 = if (page.violation_column) |column| std.math.cast(u16, column) orelse return error.InvalidSchemaValidationJob else null;
        const finished = if (self.coverage == .prefix) records == self.header.records else !page.page.more;
        if (violation != null and (self.header.phase != .failed or !finished)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.coverage == .prefix and !finished and !page.page.more) return error.SchemaValidationRuntimeRecoveryRequired;
        if (finished) {
            if (self.header.phase != .released and !std.meta.eql(reference_coverage, self.header.reference_coverage)) return error.SchemaValidationRuntimeRecoveryRequired;
            if (self.coverage == .prefix or self.header.phase != .released) {
                if (records != self.header.records or examined != self.header.rows_scanned or
                    !eq(cursor, self.header.cursor_digest) or violation != self.header.failure_column)
                    return error.SchemaValidationRuntimeRecoveryRequired;
            } else if (violation != null) return error.SchemaValidationRuntimeRecoveryRequired;
        }
        self.records = records;
        self.rows_examined = examined;
        self.reference_coverage = reference_coverage;
        self.cursor_digest = cursor;
        self.done = finished;
        return finished;
    }

    pub fn summary(self: *const @This()) !Summary {
        if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
        if (!self.done) return error.SchemaValidationNotReady;
        return self.snapshotSummary();
    }

    /// Verify the current live definition before returning cached coverage.
    /// Normal post-release data writes may advance the actual storage clock;
    /// a changed job, schema or backwards clock must never reuse old coverage.
    pub fn requireCurrentMetadata(self: *const @This(), a: A, core: anytype, control: projection.Control) !void {
        try control.ensureActive();
        var read = blk: {
            core.lockApplyShared();
            defer core.unlockApplyShared();
            break :blk try core.store.beginReadTxn();
        };
        defer read.abort();
        const binding = self.header.binding;
        const current = try observe(a, &read, binding);
        if (!std.meta.eql(current, self.header)) return error.SchemaValidationJobChanged;
        if (current.phase == .released) try requireSuccessorDefinition(a, &read, binding) else if (current.installation == null) try requireCurrent(binding, &read);
        try precedesOrEqual(self.snapshot_cut, try currentPosition(&read, binding));
        try control.ensureActive();
    }

    fn snapshotSummary(self: *const @This()) Summary {
        return .{ .header = self.header, .snapshot_cut = self.snapshot_cut, .coverage = self.coverage, .records = self.records, .rows = self.rows_examined, .cursor_digest = self.cursor_digest, .reference_coverage = self.reference_coverage };
    }
};

/// Trusted independently executed read preparation. Encoded page claims alone
/// are never accepted by stagePage; transport receivers must use replay.
pub const PreparedPage = struct {
    page: rows.NullabilityPage,
    expected: Header,
    reference_coverage: ?contract.ReferenceCoverage = null,
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
        return .{ .binding = self.expected.binding, .expected = try self.expected.digest(), .boundary = .{ .records_examined = self.page.page.records_examined, .rows_examined = self.page.rows_examined, .violation_column = self.page.violation_column, .after = self.page.after, .more = self.page.page.more }, .reference_coverage = self.reference_coverage };
    }
    fn prepareMode(a: A, core: anytype, binding: Binding, budget: rows.Budget, control: projection.Control, replay_command: ?PageCommand) !PreparedPage {
        if (binding.purpose == .foreign_key_parent or binding.purpose == .foreign_key_dual) return error.ForeignKeyValidationRequired;
        try control.ensureActive();
        var reader = try openPhysicalReader(a, core);
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
        if (candidate.foreign_source) |*source| {
            try reader.resumeForeignKeyValidationScan(source, .{ .projection_identity = binding.projection_identity, .after = cursor }, control);
            const page = if (replay_command) |expected|
                try reader.nextForeignKeyValidationReplayPage(a, null, .{ .scan = expected.boundary, .witness = expected.reference_coverage.?.expected }, source, control)
            else
                try reader.nextForeignKeyValidationPage(a, null, budget, source, control);
            return .{ .page = page.scan, .expected = header, .reference_coverage = .{ .expected = page.witness } };
        }
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
    if ((header.reference_coverage == null) != (prepared.reference_coverage == null)) return error.InvalidSchemaValidationJob;
    if (prepared.reference_coverage) |delta| {
        try header.reference_coverage.?.expected.combine(delta.expected);
        try header.reference_coverage.?.observed.combine(delta.observed);
    }
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
    if (binding.purpose == .foreign_key_parent or binding.purpose == .foreign_key_dual) return error.ForeignKeyValidationRequired;
    try binding.validate();
    try requireNamespace(binding, txn);
    const previous = try load(txn);
    if (previous) |prior| {
        // Cancellation is terminal progress, not permission to repair an
        // unproven restored/future cut by overwriting it with a local clock.
        try requireCommitted(txn, prior);
        if (prior.phase == .installed or (prior.phase == .released and std.meta.eql(prior.binding, binding))) return error.SchemaValidationPublicationRequired;
        if (std.meta.eql(prior.binding, binding) and prior.phase == .canceled) {
            return prior;
        }
        if (!std.meta.eql(prior.binding, binding) and ((prior.phase != .canceled and prior.phase != .released) or prior.binding.fence.admission_epoch >= binding.fence.admission_epoch))
            return error.SchemaValidationJobChanged;
    }
    if (try topology.current(txn)) |actual| if (!actual.eql(binding.fence)) return error.IntegrityTopologyBusy;
    if (try topology.completed(txn)) |done| if (done.admission_epoch >= binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
    const position = try nextPosition(txn, binding, entry);
    if (previous) |prior| try precedes(prior.updated, position);
    var header: Header = if (previous != null and std.meta.eql(previous.?.binding, binding)) previous.? else .{ .binding = binding, .phase = .canceling, .admitted = null, .updated = position, .cursor_digest = hash(""), .chain = hash("schema-validation-unvisited/v1"), .last_effect = hash("schema-validation-cancel/v1"), .reference_coverage = if (binding.purpose == .not_null) null else .{} };
    header.phase = .canceling;
    header.failure_column = null;
    header.updated = position;
    header.revision = try std.math.add(u64, header.revision, 1);
    try save(a, txn, header);
    if (binding.purpose == .foreign_key_source) {
        if (try optional(txn, activation.validation_key)) |bytes| {
            var record = try activation.ValidationRecord.decode(bytes);
            if (!std.meta.eql(record.binding, binding)) {
                // A never-visited newer cancellation can coexist with inert
                // history. It must not cancel another active owner's job.
                if (record.progress.state != .invalid or record.binding.fence.admission_epoch >= binding.fence.admission_epoch)
                    return error.SchemaValidationJobChanged;
            } else {
                if (record.progress.state != .invalid and (header.admitted == null or record.progress.state == .validating))
                    return error.SchemaValidationCancellationRequired;
                record.revision = try std.math.add(u64, record.revision, 1);
                record.progress.state = .invalid;
                record.progress.failure = "Canceled";
                const canceled = try record.encode(a);
                defer a.free(canceled);
                try txn.put(activation.validation_key, canceled);
            }
        }
    }
    try topology.stageSchemaValidationCancellation(txn, manager, binding.fence);
    if (header.admitted != null) try debt.retire(txn, try debt.Scope.fromBinding(binding));
    header.phase = .canceled;
    header.last_effect = hash("schema-validation-cancel/v1");
    try save(a, txn, header);
    return header;
}
