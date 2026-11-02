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

//! One physical preparation per owner, before durable admission. The metadata
//! writer revalidates the table cut and every signed owner; preparing a plan
//! never reserves a table, installs a fence, or authorizes root replacement.
const std = @import("std");
const local = @import("antfly_local_sources");
const admission = @import("../metadata/schema_validation_admission.zig");
const plans = @import("../metadata/schema_validation_plan.zig");
const ledger = @import("../metadata/schema_validation_ledger.zig");
const attest = @import("../metadata/schema_validation_attestation.zig");
const manager = @import("../metadata/table_manager.zig");
const transport = @import("schema_validation_preparation.zig");
const recovery = @import("schema_validation_recovery.zig");
const operation = local.api_operation;
const contract = local.storage_db_relational_schema_owner_contract;

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    prepared: plans.Prepared,
    command: []u8,
    lookup: ledger.Lookup,
    recovery_token: [recovery.Token.encoded_len]u8,

    pub fn deinit(self: *@This()) void {
        self.prepared.alloc.free(self.command);
        self.prepared.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn prepare(source: anytype, owners: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, request: admission.Request) !Owned {
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const bytes = try source.systemCatalog(alloc, context, .{ .schema_validation_prepare = request });
    defer alloc.free(bytes);
    if (bytes.len == 0 or bytes.len > admission.max_wire_bytes) return error.InvalidCatalogRecord;
    var cut = try std.json.parseFromSlice(admission.Cut, alloc, bytes, .{ .allocate = .alloc_always });
    defer cut.deinit();
    return build(owners, alloc, io, context, cut.value, request);
}

/// owners.prepare returns an owned ASPR frame using the supplied allocator.
/// Every transient transport response is freed before visiting the next owner.
pub fn build(owners: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, cut: admission.Cut, request: admission.Request) !Owned {
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    try cut.requireRequest(alloc, request);
    if (comptime @hasDecl(@typeInfo(@TypeOf(owners)).pointer.child, "prepareCohort")) try owners.prepareCohort(cut.ranges, context);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    var id: [16]u8 = undefined;
    while (true) {
        try context.ensureActive();
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const provisional = contract.hash(&id);
    const transition = std.mem.readInt(u64, id[8..16], .little);
    const before = try manager.cloneTable(a, cut.before);
    const after = try manager.cloneTable(a, cut.after);
    var guard = request.guard;
    guard.target.database = try a.dupe(u8, guard.target.database);
    guard.target.namespace = try a.dupe(u8, guard.target.namespace);
    guard.target.name = try a.dupe(u8, guard.target.name);
    guard.logical_table = try a.dupe(u8, guard.logical_table);
    const selection: contract.InputSelection = switch (request.selection) {
        .object_rename => |renames| .{ .object_rename = try local.system_catalog_domain.object_annotations.cloneRenames(a, renames) },
        .not_null => |names| block: {
            const columns = try a.alloc([]const u8, names.len);
            for (names, columns) |column, *owned| owned.* = try a.dupe(u8, column);
            break :block .{ .not_null = columns };
        },
        .foreign_keys => |names| block: {
            if (comptime @hasDecl(@typeInfo(@TypeOf(owners)).pointer.child, "captureForeignKeys")) {
                var captured = try owners.captureForeignKeys(a, cut.before, names, context);
                defer captured.deinit();
                // The returned plan owns all configuration strings after the
                // transient route/catalog capture has been released.
                const bytes = try std.json.Stringify.valueAlloc(a, captured.value, .{});
                const value = try std.json.parseFromSliceLeaky(contract.ForeignKeySource, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
                break :block .{ .foreign_keys = value };
            } else return error.ForeignKeyValidationRequired;
        },
    };
    try selection.requireSelection(request.selection);
    const intents = try a.alloc(plans.OwnerIntent, cut.ranges.len);
    const preparations = try a.alloc(attest.SignedPreparation, cut.ranges.len);
    var inputs: std.ArrayList([]const u8) = .empty;
    var digests: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer digests.deinit(alloc);
    var known: [transport.max_known_inputs][32]u8 = undefined;
    var known_len: usize = 0;
    var input_bytes: usize = 0;
    for (cut.ranges, intents, preparations) |range, *intent, *signed| {
        try context.ensureActive();
        const query: transport.Request = .{
            .group_id = range.group_id,
            .metadata_incarnation = cut.metadata_incarnation,
            .id = id,
            .provisional_plan = provisional,
            .transition_id = transition,
            .namespace = .{ .table_id = before.table_id, .shard_id = manager.rangeDocIdentityShardId(range), .range_id = manager.rangeDocIdentityRangeId(range) },
            .before_schema = before.schema_json,
            .after_schema = after.schema_json,
            .selection = selection,
            .known_inputs = known[0..known_len],
        };
        const frame = try owners.prepare(alloc, range.group_id, before.name, query, context);
        defer alloc.free(frame);
        const value = try transport.decode(frame);
        try value.requireFor(range.group_id, query);
        if ((value.signed.value.binding.authority == .native) != (cut.authority == .native)) return error.SchemaValidationJobChanged;
        const digest = value.signed.value.binding.inputs;
        const entry = try digests.getOrPut(alloc, digest);
        if (!entry.found_existing) {
            // An omitted frame is legal only when already present. Validate
            // byte ownership as well as the transport's bounded hint list.
            if (value.inputs.len == 0) return error.SchemaValidationJobChanged;
            input_bytes = std.math.add(usize, input_bytes, value.inputs.len + 4) catch return error.TransactionTooLarge;
            if (input_bytes > ledger.max_command_bytes - 48) return error.TransactionTooLarge;
            try inputs.append(a, try a.dupe(u8, value.inputs));
            if (known_len < known.len) {
                known[known_len] = digest;
                known_len += 1;
            }
        }
        signed.* = value.signed;
        intent.* = .{ .range = try manager.cloneRange(a, range), .binding = value.signed.value.binding, .physical_root_incarnation = value.signed.value.physical_root_incarnation };
    }
    const plan: plans.Plan = .{ .metadata_group_id = cut.metadata_group_id, .metadata_incarnation = cut.metadata_incarnation, .id = id, .table_guard = guard, .before = before, .after = after, .selection = selection, .owners = intents };
    var prepared = try plans.Prepared.init(alloc, plan);
    errdefer prepared.deinit();
    // Preparation signatures intentionally normalize only the provisional plan
    // digest. All physical/input/fence facts stay signed; finalizing the cohort
    // therefore needs neither a second owner RPC nor a second compilation.
    for (intents, prepared.owners, preparations) |*intent, owner, signed| {
        intent.binding = owner.binding;
        try signed.verifyFor(owner.binding, owner.physical_root_incarnation);
    }
    try context.ensureActive();
    const command = try ledger.encodeBegin(alloc, .{ .plan = plan, .preparations = preparations, .inputs = inputs.items });
    errdefer alloc.free(command);
    const lookup: ledger.Lookup = .fromHead(prepared.head);
    return .{ .arena = arena, .prepared = prepared, .command = command, .lookup = lookup, .recovery_token = try recovery.Token.encode(lookup) };
}

/// Prepare one additive enum epoch over the complete catalog-owned dependency
/// cut. Each owner compiles and signs its own actual native layout/catalog;
/// requests carry neither caller-selected roots nor installation authority.
pub fn buildType(owners: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, cut: @import("../system_catalog/named_types.zig").Snapshot, request: @import("../system_catalog/named_types.zig").Mutation) !Owned {
    const types = @import("../system_catalog/named_types.zig");
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    if (request.operation != .add_enum and request.operation != .set_domain_not_null) return error.InvalidGenerationPublication;
    const activation = cut.activation orelse return error.InvalidGenerationPublication;
    const previous = (cut.found orelse return error.SqlNamedTypeNotFound).record;
    try cut.validate(alloc, .{ .targets = &.{}, .reference = .{ .id = previous.identity.id, .namespace_id = previous.identity.namespace_id }, .include_dependents = true, .prepare_activation = true });
    const expected = switch (request.operation) {
        .add_enum => |change| change.expected,
        .set_domain_not_null => |change| change.expected,
        else => unreachable,
    };
    if (cut.dependents.len == 0 or previous.identity.id != expected.id or
        previous.identity.generation != expected.generation or previous.identity.namespace_id != request.namespace_id or
        !std.mem.eql(u8, &request.incarnation, &cut.incarnation)) return error.CatalogGenerationChanged;
    if (comptime @hasDecl(@typeInfo(@TypeOf(owners)).pointer.child, "prepareCohort")) try owners.prepareCohort(activation.ranges, context);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    // Own the small immutable definition and intent independently of the
    // transient metadata snapshot. Table schemas are cloned once below.
    const previous_bytes = try previous.encode(a);
    const before_type = (try types.records.Record.decode(a, previous_bytes)).value;
    const request_bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    const intent_request = try std.json.parseFromSliceLeaky(types.Mutation, a, request_bytes, .{ .allocate = .alloc_always });
    var after_type = before_type;
    after_type.identity.generation = std.math.add(u32, before_type.identity.generation, 1) catch return error.CatalogRevisionExhausted;
    switch (request.operation) {
        .add_enum => |add| {
            const added = try types.records.Owned.add(a, before_type.definition, add.label, add.placement);
            after_type.definition = added.definition;
        },
        .set_domain_not_null => {
            if (before_type.definition != .domain) return error.SqlWrongObjectType;
            after_type.definition.domain.not_null = true;
        },
        else => unreachable,
    }
    try before_type.requireEpochSuccessor(after_type);
    const epoch: contract.EnumEpoch = .{ .type_id = before_type.identity.id, .generation = before_type.identity.generation, .origin_id = before_type.identity.origin_id };
    const selection: contract.InputSelection = if (request.operation == .set_domain_not_null) .{ .domain_not_null = epoch } else .{ .enum_epoch = epoch };
    var id: [16]u8 = undefined;
    while (true) {
        try context.ensureActive();
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const provisional = contract.hash(&id);
    const transition = std.mem.readInt(u64, id[8..16], .little);
    const pairs = try a.alloc(plans.TablePair, cut.dependents.len);
    var pair_bytes: u64 = 0;
    for (cut.dependents, pairs) |table, *pair| {
        pair.before = try manager.cloneTable(a, table);
        pair.after = try manager.cloneTable(a, table);
        const schema = try local.sql_catalog.named_layouts.epochSchemaAlloc(a, table.schema_json, before_type, after_type);
        a.free(pair.after.schema_json);
        pair.after.schema_json = schema;
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(pair.*, .{}, &count.writer);
        pair_bytes +|= count.fullCount();
        if (pair_bytes > plans.max_bytes) return error.TransactionTooLarge;
    }
    const captured = try prepareCatalogOwners(owners, alloc, a, context, activation, pairs, selection, id, provisional, transition);
    const intents = captured.intents;
    const preparations = captured.preparations;
    const plan: plans.Plan = .{ .version = 3, .metadata_group_id = activation.metadata_group_id, .metadata_incarnation = activation.metadata_incarnation, .id = id, .before = pairs[0].before, .after = pairs[0].after, .additional_tables = pairs[1..], .selection = selection, .owners = intents, .type_change = .{ .request = intent_request, .before = before_type, .after = after_type } };
    var prepared = try plans.Prepared.init(alloc, plan);
    errdefer prepared.deinit();
    for (intents, prepared.owners, preparations) |*intent, owner, signed| {
        intent.binding = owner.binding;
        try signed.verifyFor(owner.binding, owner.physical_root_incarnation);
    }
    try context.ensureActive();
    const command = try ledger.encodeBegin(alloc, .{ .plan = plan, .preparations = preparations, .inputs = captured.inputs });
    errdefer alloc.free(command);
    const lookup: ledger.Lookup = .fromHead(prepared.head);
    return .{ .arena = arena, .prepared = prepared, .command = command, .lookup = lookup, .recovery_token = try recovery.Token.encode(lookup) };
}

const CatalogOwners = struct { intents: []plans.OwnerIntent, preparations: []attest.SignedPreparation, inputs: []const []const u8 };
fn prepareCatalogOwners(owners: anytype, alloc: std.mem.Allocator, a: std.mem.Allocator, context: operation.RequestContext, activation: @import("../metadata/schema_validation_publication.zig").OwnerCut, pairs: []const plans.TablePair, selection: contract.InputSelection, id: [16]u8, provisional: [32]u8, transition: u64) !CatalogOwners {
    const intents = try a.alloc(plans.OwnerIntent, activation.ranges.len);
    const preparations = try a.alloc(attest.SignedPreparation, activation.ranges.len);
    var inputs: std.ArrayList([]const u8) = .empty;
    var digests: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer digests.deinit(alloc);
    var known: [transport.max_known_inputs][32]u8 = undefined;
    var known_len: usize = 0;
    var input_bytes: usize = 0;
    var table_index: usize = 0;
    for (activation.ranges, intents, preparations) |range, *intent, *signed| {
        try context.ensureActive();
        while (table_index < pairs.len and pairs[table_index].before.table_id < range.table_id) table_index += 1;
        if (table_index == pairs.len or pairs[table_index].before.table_id != range.table_id) return error.InvalidGenerationPublication;
        const pair = pairs[table_index];
        const query: transport.Request = .{ .group_id = range.group_id, .metadata_incarnation = activation.metadata_incarnation, .id = id, .provisional_plan = provisional, .transition_id = transition, .namespace = .{ .table_id = range.table_id, .shard_id = manager.rangeDocIdentityShardId(range), .range_id = manager.rangeDocIdentityRangeId(range) }, .before_schema = pair.before.schema_json, .after_schema = pair.after.schema_json, .selection = selection, .known_inputs = known[0..known_len] };
        const frame = try owners.prepare(alloc, range.group_id, pair.before.name, query, context);
        defer alloc.free(frame);
        const value = try transport.decode(frame);
        try value.requireFor(range.group_id, query);
        if (value.signed.value.binding.authority != activation.authority) return error.SchemaValidationJobChanged;
        const digest = value.signed.value.binding.inputs;
        const entry = try digests.getOrPut(alloc, digest);
        if (!entry.found_existing) {
            if (value.inputs.len == 0) return error.SchemaValidationJobChanged;
            input_bytes = std.math.add(usize, input_bytes, value.inputs.len + 4) catch return error.TransactionTooLarge;
            if (input_bytes > ledger.max_command_bytes - 48) return error.TransactionTooLarge;
            try inputs.append(a, try a.dupe(u8, value.inputs));
            if (known_len < known.len) {
                known[known_len] = digest;
                known_len += 1;
            }
        }
        signed.* = value.signed;
        intent.* = .{ .range = try manager.cloneRange(a, range), .binding = value.signed.value.binding, .physical_root_incarnation = value.signed.value.physical_root_incarnation };
    }
    return .{ .intents = intents, .preparations = preparations, .inputs = inputs.items };
}

/// Ordinary sequence CASCADE changes defaults, never stored row values. The
/// metadata writer rechecks this complete cut and owns the catalog finalizer.
pub fn buildSequenceRetirement(owners: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, cut: @import("../system_catalog/sequence_catalog.zig").Snapshot, request: @import("../system_catalog/sequence_catalog.zig").Mutation) !Owned {
    const sequences = @import("../system_catalog/sequence_catalog.zig");
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    try request.validate();
    const expected = if (request.operation == .drop and request.operation.drop.cascade) request.operation.drop.expected orelse return error.InvalidGenerationPublication else return error.InvalidGenerationPublication;
    try cut.validate(.{ .targets = &.{request.target}, .sequence_ids = &.{expected.id}, .incarnation = request.incarnation, .prepare_retirement = true });
    const retirement = cut.retirement orelse return error.InvalidGenerationPublication;
    const activation = retirement.activation orelse return error.InvalidGenerationPublication;
    if (retirement.dependents.len == 0 or cut.entries[0].value != .sequence or cut.entries[0].namespace_id != request.namespace_id) return error.CatalogGenerationChanged;
    const previous = cut.entries[0].value.sequence;
    if (previous.internal_identity) return error.SqlDependentObjectsExist;
    if (previous.id != expected.id or previous.generation != expected.generation) return error.CatalogGenerationChanged;
    if (comptime @hasDecl(@typeInfo(@TypeOf(owners)).pointer.child, "prepareCohort")) try owners.prepareCohort(activation.ranges, context);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const request_bytes = try std.json.Stringify.valueAlloc(a, request, .{});
    const owned_request = try std.json.parseFromSliceLeaky(sequences.Mutation, a, request_bytes, .{ .allocate = .alloc_always });
    const change: plans.SequenceChange = .{ .request = owned_request, .before = previous, .database_id = retirement.database_id, .references = retirement.references };
    const selection: contract.InputSelection = .{ .sequence_default_removal = change.selector() };
    var id: [16]u8 = undefined;
    while (true) {
        try context.ensureActive();
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const pairs = try a.alloc(plans.TablePair, retirement.dependents.len);
    var pair_bytes: u64 = 0;
    for (retirement.dependents, pairs) |table, *pair| {
        try context.ensureActive();
        pair.before = try manager.cloneTable(a, table);
        pair.after = try manager.cloneTable(a, table);
        const schema = try change.selector().schemaAlloc(a, table.table_id, table.schema_json);
        a.free(pair.after.schema_json);
        pair.after.schema_json = schema;
        var count = std.Io.Writer.Discarding.init(&.{});
        try std.json.Stringify.value(pair.*, .{}, &count.writer);
        pair_bytes +|= count.fullCount();
        if (pair_bytes > plans.max_bytes) return error.TransactionTooLarge;
    }
    const captured = try prepareCatalogOwners(owners, alloc, a, context, activation, pairs, selection, id, contract.hash(&id), std.mem.readInt(u64, id[8..16], .little));
    const plan: plans.Plan = .{ .version = 4, .metadata_group_id = activation.metadata_group_id, .metadata_incarnation = activation.metadata_incarnation, .id = id, .before = pairs[0].before, .after = pairs[0].after, .additional_tables = pairs[1..], .selection = selection, .owners = captured.intents, .sequence_change = change };
    var prepared = try plans.Prepared.init(alloc, plan);
    errdefer prepared.deinit();
    for (captured.intents, prepared.owners, captured.preparations) |*intent, owner, signed| {
        intent.binding = owner.binding;
        try signed.verifyFor(owner.binding, owner.physical_root_incarnation);
    }
    try context.ensureActive();
    const command = try ledger.encodeBegin(alloc, .{ .plan = plan, .preparations = captured.preparations, .inputs = captured.inputs });
    errdefer alloc.free(command);
    const lookup: ledger.Lookup = .fromHead(prepared.head);
    return .{ .arena = arena, .prepared = prepared, .command = command, .lookup = lookup, .recovery_token = try recovery.Token.encode(lookup) };
}

/// Prepare the complete surviving-schema cohort after the accepted root's
/// physical parent coverage. Owner signatures come from fresh native catalog
/// cuts; the metadata writer independently rejoins the immutable root effects.
pub fn buildTableRetirement(owners: anytype, alloc: std.mem.Allocator, io: std.Io, context: operation.RequestContext, head: @import("../metadata/table_retirement_ledger.zig").Head, manifest: @import("../metadata/table_retirement_manifest.zig").Manifest, authority_kind: local.storage_source_authority.Kind) !Owned {
    const retirement = @import("../metadata/table_retirement_ledger.zig");
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    try head.validate();
    try manifest.validate(alloc);
    const root = try retirement.authority(head);
    if (head.phase != .physical_ready or !std.meta.eql(root.effects, try manifest.reference()) or
        root.root.group != manifest.metadata_group_id or !std.mem.eql(u8, &head.incarnation, &manifest.metadata_incarnation)) return error.GenerationPublicationChanged;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    // Own the selection's variable strings independently of the caller's
    // manifest arena, which can be released after this function returns.
    const selector_json = try std.json.Stringify.valueAlloc(a, manifest.selection, .{});
    const selector = try std.json.parseFromSliceLeaky(local.system_catalog_dependency_retirement.Selection, a, selector_json, .{ .allocate = .alloc_always });
    const selection: contract.InputSelection = .{ .dependent_object_retirement = selector };
    const pairs = try a.alloc(plans.TablePair, manifest.survivors.len);
    for (manifest.survivors, pairs) |pair, *owned| owned.* = .{ .before = try manager.cloneTable(a, pair.before), .after = try manager.cloneTable(a, pair.after) };
    var id: [16]u8 = undefined;
    while (true) {
        try context.ensureActive();
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0 and !std.mem.eql(u8, &id, &head.lookup.id)) break;
    }
    const ranges = try a.dupe(local.common_topology_records.RangeRecord, manifest.survivor_ranges);
    std.mem.sort(local.common_topology_records.RangeRecord, ranges, {}, struct {
        fn less(_: void, left: local.common_topology_records.RangeRecord, right: local.common_topology_records.RangeRecord) bool {
            if (left.table_id != right.table_id) return left.table_id < right.table_id;
            return std.mem.order(u8, left.start_key, right.start_key) == .lt;
        }
    }.less);
    if (comptime @hasDecl(@typeInfo(@TypeOf(owners)).pointer.child, "prepareCohort")) try owners.prepareCohort(ranges, context);
    const activation: @import("../metadata/schema_validation_publication.zig").OwnerCut = .{ .metadata_group_id = manifest.metadata_group_id, .metadata_incarnation = manifest.metadata_incarnation, .ranges = ranges, .authority = authority_kind };
    const captured = try prepareCatalogOwners(owners, alloc, a, context, activation, pairs, selection, id, contract.hash(&id), std.mem.readInt(u64, id[8..16], .little));
    const plan: plans.Plan = .{ .version = 5, .metadata_group_id = manifest.metadata_group_id, .metadata_incarnation = manifest.metadata_incarnation, .id = id, .before = pairs[0].before, .after = pairs[0].after, .additional_tables = pairs[1..], .selection = selection, .owners = captured.intents, .retirement_root = root };
    var prepared = try plans.Prepared.init(alloc, plan);
    errdefer prepared.deinit();
    for (captured.intents, prepared.owners, captured.preparations) |*intent, owner, signed| {
        intent.binding = owner.binding;
        try signed.verifyFor(owner.binding, owner.physical_root_incarnation);
    }
    try context.ensureActive();
    const command = try ledger.encodeBegin(alloc, .{ .plan = plan, .preparations = captured.preparations, .inputs = captured.inputs });
    errdefer alloc.free(command);
    const lookup: ledger.Lookup = .fromHead(prepared.head);
    return .{ .arena = arena, .prepared = prepared, .command = command, .lookup = lookup, .recovery_token = try recovery.Token.encode(lookup) };
}
