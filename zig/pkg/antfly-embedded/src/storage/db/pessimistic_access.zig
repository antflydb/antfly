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

//! Native mutation-side conflict checks under the existing apply mutex. The
//! receiving owner derives identity and key-changing strength independently of
//! SQL. Canonical typed tuples reuse UNIQUE's equality codec; source JSON is
//! never reparsed to classify a prepared relational write.
const std = @import("std");
const binding_mod = @import("../pessimistic_lock_resource.zig");
const scopes = @import("../pessimistic_lock_authority.zig");
const ledger = scopes.ledger;
const contracts = @import("../pessimistic_lock_contract.zig");
const store_mod = @import("../docstore.zig");
const transactions = @import("../transactions.zig");
const schemas = @import("schema_registry.zig");
const rows = @import("algebraic/relational_row_codec.zig");
const tuples = @import("relational_index_keys.zig");
const native = @import("../relational_index.zig");
const A = std.mem.Allocator;

pub const Guard = struct {
    a: A,
    read: ?store_mod.DocStore.Txn = null,
    binding: ?binding_mod.Binding = null,
    owner: ?ledger.AccessOwner = null,
    current: ?schemas.SchemaView = null,
    historical: ?schemas.SchemaView = null,
    key_plans: std.ArrayList(tuples.TuplePlan) = .empty,
    plans_ready: bool = false,
    before: std.ArrayList(u8) = .empty,
    after: std.ArrayList(u8) = .empty,

    /// The caller holds the DB apply mutex through both this probe and the
    /// eventual mutation publication. An absent binding is one point read.
    pub fn init(a: A, core: anytype, txn_id: ?transactions.TxnId) !Guard {
        var result: Guard = .{ .a = a, .read = try core.store.beginReadTxn() };
        errdefer result.deinit();
        result.binding = try binding_mod.Binding.load(&result.read.?);
        const binding = result.binding orelse {
            result.read.?.abort();
            result.read = null;
            return result;
        };
        if (try scopes.activeCount(&result.read.?, binding.namespace) == 0) {
            result.read.?.abort();
            result.read = null;
            result.binding = null;
            return result;
        }
        try binding.requireIdentity(core.identity_namespace);
        result.current = core.acquireSchemaView();
        // Exclude only the immutable native decision's independently derived
        // prepared backend. A frontend-supplied transaction ID confers no rights.
        if (txn_id) |id| if (try transactions.externalParticipant(&result.read.?, id)) |state| if (state.owner) |decision| {
            if (!std.mem.eql(u8, &decision.metadata_incarnation, &std.fmt.bytesToHex(binding.namespace.incarnation, .lower))) return error.LockOwnerChanged;
            const backend = try scopes.preparedBackend(binding.namespace, .{ .operation = decision.operation_id, .admission = decision.admission_digest });
            if (try ledger.readOwner(a, &result.read.?, binding.namespace, backend)) |held| {
                if (held.state != .prepared or !std.meta.eql(held.transaction, @as(?[16]u8, id)) or held.job != null or held.waiting != null) return error.LockOwnerBusy;
                result.owner = .{ .backend = backend, .transaction = id };
            }
        };
        return result;
    }
    pub fn deinit(self: *Guard) void {
        for (self.key_plans.items) |*plan| plan.deinit();
        self.key_plans.deinit(self.a);
        self.before.deinit(self.a);
        self.after.deinit(self.a);
        if (self.historical) |*view| view.release();
        if (self.current) |*view| view.release();
        if (self.read) |*read| read.abort();
        self.* = undefined;
    }
    pub fn table(self: *Guard, mode: contracts.TableMode) !void {
        const binding = self.binding orelse return;
        if (try ledger.accessBlocked(self.a, &self.read.?, binding.namespace, try binding.table(), .{ .table = mode }, self.owner)) return error.IntentConflict;
    }
    pub fn row(self: *Guard, key_value: []const u8, mode: contracts.RowMode) !void {
        const binding = self.binding orelse return;
        if (std.mem.startsWith(u8, key_value, "\x00\x00")) return;
        if (try ledger.accessBlocked(self.a, &self.read.?, binding.namespace, try binding.row(key_value), .{ .row = mode }, self.owner)) return error.IntentConflict;
    }
    fn preparePlans(self: *Guard) !void {
        if (self.plans_ready) return;
        const current = self.current orelse {
            self.plans_ready = true;
            return;
        };
        const validator = current.validator() orelse {
            self.plans_ready = true;
            return;
        };
        if (validator.schema.storage_mode != .relational) {
            self.plans_ready = true;
            return;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const definitions = try validator.schema.relationalUniqueDefinitions(scratch.allocator());
        for (definitions) |unique| {
            // These are exactly the plain, immediate UNIQUE targets usable by
            // FK references. Partial/expression/deferred keys do not make a
            // non-key UPDATE conflict with FOR KEY SHARE.
            if (unique.columns.len == 0 or unique.keys.len != 0 or unique.expressions.len != 0 or unique.where.len != 0 or unique.where_expressions.len != 0 or unique.deferrable or unique.without_overlaps_column != null) continue;
            const keys = try scratch.allocator().alloc(native.RelationalIndexKey, unique.columns.len);
            for (keys, unique.columns) |*key, column| key.* = .{ .column = column };
            var plan = try tuples.TuplePlan.init(self.a, current.tableSchema().*, current.physicalLayout(), keys);
            errdefer plan.deinit();
            try self.key_plans.append(self.a, plan);
        }
        self.plans_ready = true;
    }
    fn viewFor(self: *Guard, core: anytype, version: u32) !schemas.SchemaView {
        if (self.current) |view| if (view.version() == version) return view;
        if (self.historical) |view| if (view.version() == version) return view;
        if (self.historical) |*view| view.release();
        self.historical = null;
        self.historical = try core.acquireSchemaVersionView(version) orelse return error.RelationalRowSchemaMismatch;
        return self.historical.?;
    }
    /// NULL denotes deletion. Inserts take UPDATE strength; an existing row
    /// whose FK-eligible unique tuple is unchanged takes NO KEY UPDATE.
    pub fn write(self: *Guard, core: anytype, key_value: []const u8, prepared: ?[]const u8) !void {
        if (self.binding == null or std.mem.startsWith(u8, key_value, "\x00\x00")) return;
        if (prepared == null) return self.row(key_value, .update);
        const current = self.current;
        if (current == null or current.?.storageMode() != .relational) return self.row(key_value, .no_key_update);
        try self.preparePlans();
        if (self.key_plans.items.len == 0) return self.row(key_value, .no_key_update);
        const store_key = try @import("relational_store.zig").keyAlloc(self.a, key_value);
        defer self.a.free(store_key);
        const previous = self.read.?.get(store_key) catch |err| switch (err) {
            error.NotFound => return self.row(key_value, .update),
            else => return err,
        };
        // Native prepared rows already own their immutable physical layout.
        // Pin each historical epoch before interpreting its schema ordinals.
        const next_version = try rows.rowSchemaVersion(prepared.?);
        const previous_version = try rows.rowSchemaVersion(previous);
        const next_view = try self.viewFor(core, next_version);
        var owned_next = next_view.clone();
        defer owned_next.release();
        const previous_view = try self.viewFor(core, previous_version);
        const next_row = try rows.ordinalRowViewTrusted(prepared.?, owned_next.tableSchema().*, owned_next.physicalLayout());
        const previous_row = try rows.ordinalRowViewSelective(previous, previous_view.tableSchema().*, previous_view.physicalLayout());
        for (self.key_plans.items) |plan| {
            self.before.clearRetainingCapacity();
            self.after.clearRetainingCapacity();
            var old_projection: ?tuples.TuplePlan = if (plan.layout != previous_view.physicalLayout()) try plan.projectSource(self.a, previous_view.tableSchema().*, previous_view.physicalLayout()) else null;
            defer if (old_projection) |*projection| projection.deinit();
            var new_projection: ?tuples.TuplePlan = if (plan.layout != owned_next.physicalLayout()) try plan.projectSource(self.a, owned_next.tableSchema().*, owned_next.physicalLayout()) else null;
            defer if (new_projection) |*projection| projection.deinit();
            _ = try (old_projection orelse plan).append(self.a, &self.before, previous_row);
            _ = try (new_projection orelse plan).append(self.a, &self.after, next_row);
            if (!std.mem.eql(u8, self.before.items, self.after.items)) return self.row(key_value, .update);
        }
        return self.row(key_value, .no_key_update);
    }
};
