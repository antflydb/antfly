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

//! Native FK publication under the database API fence. The durable root plan
//! is the local metadata decision; owner transitions retain their own exact
//! receipts. Recovery completes that decision before admitting another call.
const h = @import("handles.zig");
const std = h.std;
const d = h.antfly.capi_dependencies;
const topology = d.storage_db_relational_integrity_topology_contract;
const catalog = d.storage_db_relational_integrity_catalog;
const Transition = @typeInfo(@typeInfo(@FieldType(topology.Command, "child_generations")).optional.child).pointer.child;
const key = "\x00antfly/embedded/sql-fk-publication";
const TestControl = if (h.builtin.is_test) struct {
    var interrupt_phase: ?u8 = null;
} else struct {};

pub fn interruptAfterPhaseForTest(phase: ?u8) void {
    std.debug.assert(h.builtin.is_test);
    if (h.builtin.is_test) TestControl.interrupt_phase = phase;
}
const Parent = struct { name: []const u8, fence: topology.Fence, transitions: []const Transition };
const Job = struct {
    child: []const u8,
    fence: topology.Fence,
    install: topology.ChildSchemaInstall,
    parents: []const Parent,
    drop: bool = false,
    create: bool = false,
    phase: u8 = 0,
};

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

fn ownerFence(db: *h.db_mod.DB, alloc: std.mem.Allocator, id: [16]u8, role: topology.Role) !topology.Fence {
    const response = (try db.lookup(alloc, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" })) orelse return error.IntegrityCatalogUnavailable;
    const observed = try std.json.parseFromSliceLeaky(topology.Identity, alloc, response.json, .{ .ignore_unknown_fields = true });
    if (!observed.namespace.eql(db.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    const fence: topology.Fence = .{
        .transition_id = std.mem.readInt(u64, id[0..8], .little),
        .attempt = std.mem.readInt(u64, id[8..16], .little),
        .admission_epoch = observed.next_epoch,
        .owner_group_id = observed.namespace.shard_id,
        .peer_group_id = observed.namespace.shard_id,
        .role = role,
        .namespace = observed.namespace,
        .catalog_digest = observed.catalog_digest,
    };
    _ = try fence.encode();
    return fence;
}

fn store(handle: *h.Handle, alloc: std.mem.Allocator, job: ?Job) !void {
    const bytes = if (job) |value| try std.json.Stringify.valueAlloc(alloc, value, .{}) else null;
    defer if (bytes) |value| alloc.free(value);
    var write = try handle.db.core.store.beginWriteTxn();
    errdefer write.abort();
    if (bytes) |value| try write.put(key, value) else try write.delete(key);
    write.commit() catch {
        handle.sql_decision_uncertain = true;
        return error.SqlMutationOutcomeUnknown;
    };
}

/// Returns false for a schema change that preserves all child FK generations.
pub fn publish(adapter: anytype, alloc: std.mem.Allocator, target: []const u8, version: u32, drop: bool) !bool {
    return publishMode(adapter, alloc, target, version, drop, false);
}

pub fn publishInitial(adapter: anytype, alloc: std.mem.Allocator, target: []const u8, version: u32) !void {
    if (!try publishMode(adapter, alloc, target, version, false, true)) return error.InvalidGenerationPublication;
}

fn publishMode(adapter: anytype, alloc: std.mem.Allocator, target: []const u8, version: u32, drop: bool, create: bool) !bool {
    const db = adapter.db;
    const handle = adapter.handle.?;
    const metadata = try adapter.localCatalog(alloc, version);
    const previous = (try db.getSchemaJson(alloc)) orelse return error.IntegrityCatalogUnavailable;
    const before = try h.tables_api.parseValidatedTableSchema(alloc, previous);
    const after = try h.tables_api.parseValidatedTableSchema(alloc, target);
    const previous_fks = try before.relationalForeignKeyDefinitions(alloc);
    const next_fks = try after.relationalForeignKeyDefinitions(alloc);
    if (previous_fks.len == 0 and next_fks.len == 0) return false;
    const runtime = try h.tables_api.deriveRuntimeTableSchema(alloc, after);
    const declarations = d.schema_relational_declarations;
    const previous_definitions = try declarations.definitionFingerprints(alloc, before, try h.tables_api.deriveRuntimeTableSchema(alloc, before));
    const next_definitions = try declarations.definitionFingerprints(alloc, after, runtime);
    if (previous_fks.len == next_fks.len) {
        const unchanged = for (previous_definitions) |prior| {
            if (prior.kind != .foreign_key) continue;
            const retained = for (next_definitions) |next| {
                if (next.kind == .foreign_key and std.mem.eql(u8, prior.name, next.name) and std.mem.eql(u8, &prior.fingerprint, &next.fingerprint)) break true;
            } else false;
            if (!retained) break false;
        } else true;
        // Ordinary UNIQUE retirement owns its preflight and native proof.
        // Preparing a published-child schema here would demand that proof
        // before the ordinary retirement coordinator has run.
        if (unchanged) return false;
    }
    var prepared = try db.core.prepareSchemaMetadataPublishedChild(runtime, &.{.{ .key = "\x00\x00__metadata__:schema_json", .value = target }});
    var prepared_live = true;
    defer if (prepared_live) prepared.deinit();
    const update = prepared.integrity_catalog orelse return false;
    var old = try catalog.decode(alloc, update.expected orelse return error.IntegrityCatalogUnavailable);
    defer old.deinit();
    var id: [16]u8 = undefined;
    const io = db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
    while (true) {
        std.Io.random(io, &id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const decision = digest(target);
    var parents: std.ArrayList(Parent) = .empty;
    for (next_fks) |fk| {
        const parent = for (metadata.table) |record| {
            if (std.mem.eql(u8, record.name, fk.parent_table)) break record;
        } else return error.UndefinedTable;
        d.schema_relational_foreign_key_target.validate(alloc, target, parent.name, if (parent.table_id == db.core.identity_namespace.table_id) target else parent.schema_json) catch |err| {
            if (h.builtin.is_test) std.debug.print("FK plan validation {s}: child={s} parent={s}\n", .{ @errorName(err), target, parent.schema_json });
            return err;
        };
        if (fk.match == .partial) {
            // Definition coverage alone is insufficient: witness reads need
            // locally verified index coverage before this publication is
            // admitted. No durable FK decision exists at this point.
            const parent_db = try @import("tables.zig").get(handle, parent.name);
            const parent_schema = try h.tables_api.parseValidatedTableSchema(alloc, if (parent_db == db) target else parent.schema_json);
            const names = try d.schema_relational_witness_indexes.supportNames(alloc, parent_schema, fk.parent_columns);
            parent_db.ensureRelationalIndexesReady(names, .none, h.antfly.platform_time.monotonicNs() +| 5 * std.time.ns_per_s) catch |err| switch (err) {
                error.DeadlineExceeded, error.RelationalIndexNotReady => return error.SqlStatementReadUnavailable,
                else => return err,
            };
        }
    }
    // Schema translation preserves unchanged definitions. Match by immutable
    // generation, so an unrelated column/index edit never republishes an FK.
    for ([_][]const @TypeOf(previous_fks[0]){ previous_fks, next_fks }, 0..) |definitions, pass| {
        for (definitions) |fk| {
            const prior = if (pass == 0) old.find(.foreign_key, fk.name) else null;
            const same_parent = if (pass == 0) for (next_fks) |candidate| {
                if (std.mem.eql(u8, candidate.name, fk.name)) break std.mem.eql(u8, candidate.parent_table, fk.parent_table);
            } else false else for (previous_fks) |candidate| {
                if (std.mem.eql(u8, candidate.name, fk.name)) break std.mem.eql(u8, candidate.parent_table, fk.parent_table);
            } else false;
            const next = if (pass == 0 and !same_parent) null else update.catalog.find(.foreign_key, fk.name);
            if (pass == 0 and prior != null and next != null and std.mem.eql(u8, &prior.?.generation, &next.?.generation)) continue;
            if (pass == 1 and same_parent) continue;
            if (prior == null and next == null) return error.IntegrityCatalogChanged;
            const record = for (metadata.table) |record| {
                if (std.mem.eql(u8, record.name, fk.parent_table)) break record;
            } else return error.UndefinedTable;
            const parent_index = for (parents.items, 0..) |parent, i| {
                if (std.mem.eql(u8, parent.name, record.name)) break i;
            } else add: {
                const parent_db = try @import("tables.zig").get(handle, record.name);
                try parents.append(alloc, .{ .name = record.name, .fence = try ownerFence(parent_db, alloc, id, if (parent_db == db) .child_generation_dual else .child_generation_parent), .transitions = &.{} });
                break :add parents.items.len - 1;
            };
            const parent = &parents.items[parent_index];
            const entries = try alloc.alloc(Transition, parent.transitions.len + 1);
            @memcpy(entries[0..parent.transitions.len], parent.transitions);
            entries[parent.transitions.len] = .{ .child_table_id = db.core.identity_namespace.table_id, .child_table_name = adapter.table_name, .constraint_name = fk.name, .expected_generation = if (prior) |binding| binding.generation else null, .next_generation = if (next) |binding| binding.generation else null, .plan_id = id, .decision_digest = decision };
            parent.transitions = entries;
        }
    }
    if (parents.items.len == 0) return false;
    for (parents.items) |*parent| {
        if (parent.transitions.len > 128) return error.InvalidGenerationPublication;
        const entries = @constCast(parent.transitions);
        std.mem.sort(Transition, entries, {}, struct {
            fn less(_: void, a: Transition, b: Transition) bool {
                return std.mem.lessThan(u8, a.constraint_name, b.constraint_name);
            }
        }.less);
        for (entries) |entry| try entry.validate();
    }
    const child_fence = for (parents.items) |parent| {
        if (std.mem.eql(u8, parent.name, adapter.table_name)) break parent.fence;
    } else try ownerFence(db, alloc, id, .child_generation_source);
    const job: Job = .{
        .child = adapter.table_name,
        .fence = child_fence,
        .install = .{ .schema_json = target, .before_schema_json_digest = digest(previous), .schema_json_digest = decision, .before_catalog_digest = digest(update.expected.?), .after_catalog_digest = digest(update.value) },
        .parents = parents.items,
        .drop = drop,
        .create = create,
    };
    // The plan owns digests and transition values, not the prepared schema.
    // Release its registry lease before publication can replace that registry
    // or DROP TABLE can close the child database.
    prepared.deinit();
    prepared_live = false;
    try store(handle, alloc, job);
    finish(handle, alloc, job) catch return error.SqlDdlPending;
    return true;
}

fn finish(handle: *h.Handle, alloc: std.mem.Allocator, initial: Job) !void {
    var job = initial;
    if (job.create) try @import("tables.zig").recoverCreate(handle, job.child, job.fence.namespace.table_id);
    while (job.phase < 5) {
        const child = try @import("tables.zig").get(handle, job.child);
        if (!child.core.identity_namespace.eql(job.fence.namespace)) return error.IdentityNamespaceMismatch;
        if (job.phase == 0) try child.batchNativeFkGenerationApply(.{ .relational_topology = .{ .action = .begin, .fence = job.fence } });
        if (job.phase < 4) for (job.parents) |parent| {
            const db = try @import("tables.zig").get(handle, parent.name);
            if (!db.core.identity_namespace.eql(parent.fence.namespace)) return error.IdentityNamespaceMismatch;
            if (job.phase == 0 and db == child) continue;
            try db.batchNativeFkGenerationApply(.{ .relational_topology = .{
                .fence = parent.fence,
                .action = switch (job.phase) {
                    0 => .begin,
                    1 => .stage_child_generation,
                    2 => .activate_child_generation,
                    3 => .acknowledge_child_generation,
                    else => unreachable,
                },
                .child_generations = if (job.phase == 0) null else parent.transitions,
            } });
        };
        if (job.phase == 4) try child.batchNativeFkGenerationApply(.{ .relational_topology = .{ .action = .install_child_schema, .fence = job.fence, .child_schema_install = job.install } });
        job.phase += 1;
        if (h.builtin.is_test) if (TestControl.interrupt_phase == job.phase) {
            TestControl.interrupt_phase = null;
            return error.SqlDdlPending;
        };
        try store(handle, alloc, job);
    }
    if (job.drop) try @import("tables.zig").finishDrop(handle, job.child);
    if (job.create) try @import("tables.zig").publishCreated(handle);
    try store(handle, alloc, null);
}

pub fn recover(handle: *h.Handle) !void {
    const bytes = (try handle.db.core.getStoreValue(handle.alloc, key)) orelse return;
    defer handle.alloc.free(bytes);
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlStatementReadUnavailable;
    var job = try std.json.parseFromSlice(Job, handle.alloc, bytes, .{});
    defer job.deinit();
    if (job.value.phase > 5) return error.InvalidGenerationPublication;
    finish(handle, handle.alloc, job.value) catch |err| {
        if (h.builtin.is_test) std.debug.print("FK recovery child={s} phase={d} error={s}\n", .{ job.value.child, job.value.phase, @errorName(err) });
        return err;
    };
}
