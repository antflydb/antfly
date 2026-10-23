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
    const columns = try a.alloc([]const u8, request.columns.len);
    for (request.columns, columns) |column, *owned| owned.* = try a.dupe(u8, column);
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
            .columns = columns,
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
    const plan: plans.Plan = .{ .metadata_group_id = cut.metadata_group_id, .metadata_incarnation = cut.metadata_incarnation, .id = id, .table_guard = guard, .before = before, .after = after, .columns = columns, .owners = intents };
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
