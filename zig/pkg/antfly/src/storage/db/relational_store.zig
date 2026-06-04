// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Relational base-store facade.
//!
//! Relational rows live in their own document-scoped keyspace and are the base
//! document record for relational tables. The implementation still uses the same
//! DocStore batch transaction underneath, so writes commit atomically with the
//! rest of the DB batch while callers use a participant-shaped interface.

const std = @import("std");
const Allocator = std.mem.Allocator;

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const schema_mod = @import("../schema.zig");
const typed_dv = @import("../../section/typed_doc_values.zig");
const transactions_mod = @import("../transactions.zig");

const default_max_set_null_updates: usize = 4096;
const max_cascade_depth: usize = 64;
const max_cascade_deletes: usize = 4096;

pub const OwnedRow = struct {
    doc_key: []u8,
    row_value: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.row_value);
        self.* = undefined;
    }
};

pub const OwnedColumnValue = struct {
    doc_key: []u8,
    value_type: typed_dv.ValueType,
    is_json: bool,
    value: typed_dv.TypedValue,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        if (self.value_type == .bytes_val) alloc.free(self.value.bytes_val);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityMode = enum {
    validate,
    dry_run,
    repair,
};

pub const ForeignKeyIntegrityReport = struct {
    scanned_child_rows: u64 = 0,
    referenced_child_rows: u64 = 0,
    scanned_ref_rows: u64 = 0,
    missing_parent_rows: u64 = 0,
    missing_ref_rows: u64 = 0,
    stale_ref_rows: u64 = 0,
    repaired_ref_rows: u64 = 0,
    deleted_stale_ref_rows: u64 = 0,

    pub fn valid(self: ForeignKeyIntegrityReport) bool {
        return self.missing_parent_rows == 0 and
            self.missing_ref_rows == 0 and
            self.stale_ref_rows == 0;
    }
};

pub const ForeignKeyDeletePlanBlockReason = enum {
    none,
    restrict,
    local_set_null_limit,
    local_cascade_limit,
};

pub const ForeignKeyDeletePlan = struct {
    exists: bool = false,
    allowed: bool = true,
    block_reason: ForeignKeyDeletePlanBlockReason = .none,
    planned_set_null_updates: u64 = 0,
    planned_cascade_deletes: u64 = 0,
    planned_row_deletes: u64 = 0,
    planned_index_deletes: u64 = 0,
    planned_writes: u64 = 0,

    pub fn touchesChildren(self: ForeignKeyDeletePlan) bool {
        return self.planned_set_null_updates > 0 or self.planned_cascade_deletes > 0;
    }
};

pub const ForeignKeyIntegrityViolationKind = enum {
    missing_parent,
    missing_ref,
    stale_ref,
};

pub const ForeignKeyIntegrityTupleValue = struct {
    column: []u8,
    value: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityViolation = struct {
    kind: ForeignKeyIntegrityViolationKind,
    constraint_name: []u8,
    child_table: []u8,
    child_key: []u8,
    parent_table: []u8,
    parent_key: []u8,
    parent_values: []ForeignKeyIntegrityTupleValue = &.{},
    observed_parent_key: ?[]u8 = null,
    observed_parent_values: []ForeignKeyIntegrityTupleValue = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.constraint_name);
        alloc.free(self.child_table);
        alloc.free(self.child_key);
        alloc.free(self.parent_table);
        alloc.free(self.parent_key);
        freeForeignKeyIntegrityTupleValues(alloc, self.parent_values);
        if (self.observed_parent_key) |observed| alloc.free(observed);
        freeForeignKeyIntegrityTupleValues(alloc, self.observed_parent_values);
        self.* = undefined;
    }
};

fn freeForeignKeyIntegrityTupleValues(alloc: Allocator, values: []ForeignKeyIntegrityTupleValue) void {
    for (values) |*value| value.deinit(alloc);
    if (values.len > 0) alloc.free(values);
}

pub fn freeForeignKeyIntegrityViolations(alloc: Allocator, violations: []ForeignKeyIntegrityViolation) void {
    for (violations) |*violation| violation.deinit(alloc);
    if (violations.len > 0) alloc.free(violations);
}

pub fn rowKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    return try internal_keys.relationalRowKeyAlloc(alloc, doc_key);
}

pub const ColumnIndexPolicy = struct {
    columns: []const schema_mod.RelationalColumn = &.{},
    restrict_to_catalog: bool = false,

    pub fn all() ColumnIndexPolicy {
        return .{};
    }

    pub fn fromColumns(columns: []const schema_mod.RelationalColumn) ColumnIndexPolicy {
        return .{
            .columns = columns,
            .restrict_to_catalog = true,
        };
    }

    pub fn shouldIndex(self: ColumnIndexPolicy, path: []const u8) bool {
        if (!self.restrict_to_catalog) return true;
        for (self.columns) |column| {
            if (std.mem.eql(u8, column.path, path) or std.mem.eql(u8, column.name, path)) return column.indexed;
        }
        return false;
    }
};

pub const WriteParticipant = struct {
    const PendingForeignKeyParentCheck = struct {
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
    };

    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    writes_start: usize,
    deletes_start: usize,
    owned_keys_start: usize,
    owned_values_start: usize,
    column_index_policy: ColumnIndexPolicy = ColumnIndexPolicy.all(),
    table_name: []const u8 = "",
    foreign_keys: []const schema_mod.ForeignKey = &.{},
    unique_constraints: []const schema_mod.UniqueConstraint = &.{},
    planned_delete_keys: []const []const u8 = &.{},
    parent_checks_externalized: bool = false,
    pending_fk_parent_checks: std.ArrayListUnmanaged(PendingForeignKeyParentCheck) = .empty,
    set_null_update_count: usize = 0,
    set_null_update_limit: usize = default_max_set_null_updates,
    cascade_depth: usize = 0,
    cascade_delete_count: usize = 0,
    prepared: bool = false,
    closed: bool = false,

    pub fn init(
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
    ) WriteParticipant {
        return .{
            .alloc = alloc,
            .store = store,
            .writes = writes,
            .deletes = deletes,
            .owned_keys = owned_keys,
            .owned_values = owned_values,
            .writes_start = writes.items.len,
            .deletes_start = deletes.items.len,
            .owned_keys_start = owned_keys.items.len,
            .owned_values_start = owned_values.items.len,
        };
    }

    pub fn initWithColumnIndexPolicy(
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_keys: *std.ArrayListUnmanaged([]u8),
        owned_values: *std.ArrayListUnmanaged([]u8),
        column_index_policy: ColumnIndexPolicy,
    ) WriteParticipant {
        var participant = init(alloc, store, writes, deletes, owned_keys, owned_values);
        participant.column_index_policy = column_index_policy;
        return participant;
    }

    pub fn configureForeignKeys(
        self: *WriteParticipant,
        table_name: []const u8,
        foreign_keys: []const schema_mod.ForeignKey,
        planned_delete_keys: []const []const u8,
        parent_checks_externalized: bool,
    ) void {
        self.table_name = table_name;
        self.foreign_keys = foreign_keys;
        self.planned_delete_keys = planned_delete_keys;
        self.parent_checks_externalized = parent_checks_externalized;
    }

    pub fn configureUniqueConstraints(
        self: *WriteParticipant,
        unique_constraints: []const schema_mod.UniqueConstraint,
    ) void {
        self.unique_constraints = unique_constraints;
    }

    pub fn prepareUpsert(
        self: *WriteParticipant,
        table: []const u8,
        doc_key: []const u8,
        typed_row: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) !void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        try self.prepareUniqueConstraintUpsert(doc_key, typed_row);
        try self.prepareForeignKeyUpsert(doc_key, typed_row);
        try appendUpsertWithColumnIndexPolicy(self.alloc, self.store, self.writes, self.deletes, self.owned_keys, self.owned_values, doc_key, typed_row, self.column_index_policy);
        self.prepared = true;
    }

    pub fn prepareDelete(
        self: *WriteParticipant,
        table: []const u8,
        doc_key: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) anyerror!void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        if (try self.isRowDeletePlanned(doc_key)) return;
        try self.prepareForeignKeyDelete(doc_key);
        try self.prepareUniqueConstraintDelete(doc_key);
        self.prepared = true;
    }

    pub fn commit(self: *WriteParticipant, txn_id: ?transactions_mod.TxnId, commit_version: u64) !void {
        _ = txn_id;
        _ = commit_version;
        if (self.closed) return error.ParticipantClosed;
        try self.validatePendingForeignKeyParentChecks();
        self.clearPendingForeignKeyParentChecks();
        self.closed = true;
    }

    pub fn abort(self: *WriteParticipant, txn_id: ?transactions_mod.TxnId) void {
        _ = txn_id;
        if (!self.closed) {
            for (self.owned_keys.items[self.owned_keys_start..]) |key| self.alloc.free(key);
            for (self.owned_values.items[self.owned_values_start..]) |value| self.alloc.free(value);
            self.owned_keys.shrinkRetainingCapacity(self.owned_keys_start);
            self.owned_values.shrinkRetainingCapacity(self.owned_values_start);
            self.writes.shrinkRetainingCapacity(self.writes_start);
            self.deletes.shrinkRetainingCapacity(self.deletes_start);
        }
        self.clearPendingForeignKeyParentChecks();
        self.closed = true;
    }

    pub fn get(self: *WriteParticipant, doc_key: []const u8, read_version: ?u64) !?[]u8 {
        _ = read_version;
        return try getMaterializedAlloc(self.alloc, self.store, doc_key);
    }

    pub fn scanRows(self: *WriteParticipant, lower_doc_key: []const u8, upper_doc_key: []const u8, read_version: ?u64) ![]OwnedRow {
        _ = read_version;
        return try scanRowsAlloc(self.alloc, self.store, lower_doc_key, upper_doc_key);
    }

    pub fn scanColumn(self: *WriteParticipant, column_path: []const u8, lower_doc_key: []const u8, upper_doc_key: []const u8, read_version: ?u64) ![]OwnedColumnValue {
        _ = read_version;
        return try scanColumnAlloc(self.alloc, self.store, column_path, lower_doc_key, upper_doc_key);
    }

    fn effectiveTableName(self: *const WriteParticipant) []const u8 {
        return if (self.table_name.len > 0) self.table_name else "_default";
    }

    fn prepareUniqueConstraintUpsert(self: *WriteParticipant, doc_key: []const u8, new_row: []const u8) !void {
        if (self.unique_constraints.len == 0) return;
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key);
        defer if (old_row) |row| self.alloc.free(row);

        for (self.unique_constraints) |constraint| {
            const old_value = if (old_row) |row| try uniqueConstraintTupleValueAlloc(self.alloc, row, constraint) else null;
            defer if (old_value) |value| self.alloc.free(value);
            const new_value = if (final_state_deleted) null else try uniqueConstraintTupleValueAlloc(self.alloc, new_row, constraint);
            defer if (new_value) |value| self.alloc.free(value);
            if (optionalBytesEqual(old_value, new_value)) continue;
            if (old_value) |value| {
                try self.requireNoRestrictingUniqueForeignKeyRefs(constraint, value);
                try self.appendUniqueConstraintDelete(constraint, value);
            }
            if (new_value) |value| {
                try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
                try self.appendUniqueConstraintWrite(constraint, value, doc_key);
            }
        }
    }

    fn prepareUniqueConstraintDelete(self: *WriteParticipant, doc_key: []const u8) !void {
        if (self.unique_constraints.len == 0) return;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key) orelse return;
        defer self.alloc.free(old_row);
        for (self.unique_constraints) |constraint| {
            const value = (try uniqueConstraintTupleValueAlloc(self.alloc, old_row, constraint)) orelse continue;
            defer self.alloc.free(value);
            try self.appendUniqueConstraintDelete(constraint, value);
        }
    }

    fn requireUniqueConstraintAvailable(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        doc_key: []const u8,
    ) !void {
        const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, constraint.name, encoded_value);
        defer self.alloc.free(key);
        if (batchWriteValue(self.writes.items, key)) |owner| {
            if (!std.mem.eql(u8, owner, doc_key)) return error.UniqueConstraintViolation;
            return;
        }
        const raw = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        defer self.alloc.free(raw);
        if (std.mem.eql(u8, raw, doc_key)) return;
        if (containsKey(self.planned_delete_keys, raw)) return;
        if (containsBatchDelete(self.deletes.items, key)) return;
        return error.UniqueConstraintViolation;
    }

    fn appendUniqueConstraintWrite(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        doc_key: []const u8,
    ) !void {
        const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, constraint.name, encoded_value);
        var key_owned = true;
        errdefer if (key_owned) self.alloc.free(key);
        const owner_value = try self.alloc.dupe(u8, doc_key);
        var value_owned = true;
        errdefer if (value_owned) self.alloc.free(owner_value);
        try self.owned_keys.append(self.alloc, key);
        key_owned = false;
        try self.owned_values.append(self.alloc, owner_value);
        value_owned = false;
        try self.writes.append(self.alloc, .{ .key = key, .value = owner_value });
    }

    fn appendUniqueConstraintDelete(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) !void {
        const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, constraint.name, encoded_value);
        var key_owned = true;
        errdefer if (key_owned) self.alloc.free(key);
        try self.owned_keys.append(self.alloc, key);
        key_owned = false;
        try self.deletes.append(self.alloc, key);
    }

    fn prepareForeignKeyUpsert(self: *WriteParticipant, doc_key: []const u8, new_row: []const u8) !void {
        if (self.foreign_keys.len == 0) return;
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key);
        defer if (old_row) |row| self.alloc.free(row);

        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const old_parent = if (old_row) |row| try foreignKeyReferenceValueAlloc(self.alloc, row, foreign_key) else null;
            defer if (old_parent) |value| self.alloc.free(value);
            const new_parent = if (final_state_deleted) null else try foreignKeyReferenceValueAlloc(self.alloc, new_row, foreign_key);
            defer if (new_parent) |value| self.alloc.free(value);
            if (optionalBytesEqual(old_parent, new_parent)) continue;
            if (old_parent) |parent_key| try self.appendForeignKeyRefDelete(foreign_key, parent_key, doc_key);
            if (new_parent) |parent_key| {
                if (!self.parent_checks_externalized) try self.deferForeignKeyParentCheck(foreign_key, parent_key);
                try self.appendForeignKeyRefWrite(foreign_key, parent_key, doc_key);
            }
        }
    }

    fn prepareForeignKeyDelete(self: *WriteParticipant, doc_key: []const u8) anyerror!void {
        if (self.foreign_keys.len == 0) return;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key) orelse return;
        defer self.alloc.free(old_row);
        try appendDelete(self.alloc, self.store, self.deletes, self.owned_keys, doc_key);
        try self.applySetNullPrimaryKeyForeignKeyRefs(doc_key);
        for (self.unique_constraints) |constraint| {
            const value = (try uniqueConstraintTupleValueAlloc(self.alloc, old_row, constraint)) orelse continue;
            defer self.alloc.free(value);
            try self.applySetNullUniqueForeignKeyRefs(constraint, value);
        }
        try self.applyCascadePrimaryKeyForeignKeyRefs(doc_key);
        for (self.unique_constraints) |constraint| {
            const value = (try uniqueConstraintTupleValueAlloc(self.alloc, old_row, constraint)) orelse continue;
            defer self.alloc.free(value);
            try self.applyCascadeUniqueForeignKeyRefs(constraint, value);
        }
        try self.requireNoRestrictingPrimaryKeyForeignKeyRefs(doc_key);
        for (self.unique_constraints) |constraint| {
            const value = (try uniqueConstraintTupleValueAlloc(self.alloc, old_row, constraint)) orelse continue;
            defer self.alloc.free(value);
            try self.requireNoRestrictingUniqueForeignKeyRefs(constraint, value);
        }
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const parent_key = (try foreignKeyReferenceValueAlloc(self.alloc, old_row, foreign_key)) orelse continue;
            defer self.alloc.free(parent_key);
            try self.appendForeignKeyRefDelete(foreign_key, parent_key, doc_key);
        }
    }

    pub fn prepareSetNullForeignKeyParentDelete(
        self: *WriteParticipant,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
    ) !void {
        const foreign_key = findForeignKeyByName(self.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
        if (!foreignKeyIsEnforced(foreign_key)) return error.ForeignKeyViolation;
        if (foreign_key.on_delete != .set_null) return error.ForeignKeyViolation;
        if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
        if (!foreignKeyReferencesPrimaryKey(foreign_key)) return error.ForeignKeyViolation;
        try self.applySetNullForeignKeyRefsForIdentity(foreign_key, parent_key);
    }

    pub fn prepareSetNullForeignKeyChildAction(
        self: *WriteParticipant,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        child_key: []const u8,
    ) !void {
        const foreign_key = findForeignKeyByName(self.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
        if (!foreignKeyIsEnforced(foreign_key)) return error.ForeignKeyViolation;
        if (foreign_key.on_delete != .set_null) return error.ForeignKeyViolation;
        if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
        try self.prepareSetNullForeignKeyChild(foreign_key, parent_key, child_key);
    }

    pub fn prepareCascadeForeignKeyChildAction(
        self: *WriteParticipant,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        child_key: []const u8,
    ) !void {
        const foreign_key = findForeignKeyByName(self.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
        if (!foreignKeyIsEnforced(foreign_key)) return error.ForeignKeyViolation;
        if (foreign_key.on_delete != .cascade) return error.ForeignKeyViolation;
        if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return;
        defer self.alloc.free(row);
        const current_parent = (try foreignKeyReferenceValueAlloc(self.alloc, row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, parent_key)) return;
        try self.prepareCascadeForeignKeyChild(child_key);
    }

    fn isRowDeletePlanned(self: *WriteParticipant, doc_key: []const u8) !bool {
        const row_key = try rowKeyAlloc(self.alloc, doc_key);
        defer self.alloc.free(row_key);
        return containsBatchDelete(self.deletes.items, row_key);
    }

    fn applySetNullPrimaryKeyForeignKeyRefs(self: *WriteParticipant, parent_key: []const u8) !void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .set_null) continue;
            if (!foreignKeyReferencesPrimaryKey(foreign_key)) continue;
            try self.applySetNullForeignKeyRefsForIdentity(foreign_key, parent_key);
        }
    }

    fn applySetNullUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) !void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .set_null) continue;
            if (!stringSlicesEqual(foreign_key.parent_columns, constraint.columns)) continue;
            try self.applySetNullForeignKeyRefsForIdentity(foreign_key, encoded_value);
        }
    }

    fn applySetNullForeignKeyRefsForIdentity(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8) !void {
        const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer self.alloc.free(prefix);
        const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer if (upper) |buf| self.alloc.free(buf);

        const writes_end = self.writes.items.len;
        for (self.writes.items[0..writes_end]) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareSetNullForeignKeyChild(foreign_key, parent_key, decoded.child_key);
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareSetNullForeignKeyChild(foreign_key, parent_key, decoded.child_key);
        }
    }

    fn prepareSetNullForeignKeyChild(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8, child_key: []const u8) !void {
        if (containsKey(self.planned_delete_keys, child_key)) return;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return;
        defer self.alloc.free(row);

        const current_parent = (try foreignKeyReferenceValueAlloc(self.alloc, row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, parent_key)) return;
        if (self.set_null_update_count >= self.set_null_update_limit) return error.ForeignKeyViolation;
        self.set_null_update_count += 1;

        const rewritten = try relationalRowWithoutColumnsAlloc(self.alloc, row, foreign_key.child_columns);
        var rewritten_owned = true;
        errdefer if (rewritten_owned) self.alloc.free(rewritten);
        try self.prepareUpsert(self.effectiveTableName(), child_key, rewritten, null);
        try self.owned_values.append(self.alloc, rewritten);
        rewritten_owned = false;
        try self.appendForeignKeyRefDelete(foreign_key, parent_key, child_key);
    }

    fn getPendingOrStoredRawRowAlloc(self: *WriteParticipant, doc_key: []const u8) !?[]u8 {
        const row_key = try rowKeyAlloc(self.alloc, doc_key);
        defer self.alloc.free(row_key);
        if (containsBatchDelete(self.deletes.items, row_key)) return null;
        var index = self.writes.items.len;
        while (index > 0) {
            index -= 1;
            const write = self.writes.items[index];
            if (std.mem.eql(u8, write.key, row_key)) return try self.alloc.dupe(u8, write.value);
        }
        return try getRawAlloc(self.alloc, self.store, doc_key);
    }

    fn applyCascadePrimaryKeyForeignKeyRefs(self: *WriteParticipant, parent_key: []const u8) anyerror!void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .cascade) continue;
            if (!foreignKeyReferencesPrimaryKey(foreign_key)) continue;
            try self.applyCascadeForeignKeyRefsForIdentity(foreign_key, parent_key);
        }
    }

    fn applyCascadeUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) anyerror!void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .cascade) continue;
            if (!stringSlicesEqual(foreign_key.parent_columns, constraint.columns)) continue;
            try self.applyCascadeForeignKeyRefsForIdentity(foreign_key, encoded_value);
        }
    }

    fn applyCascadeForeignKeyRefsForIdentity(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8) anyerror!void {
        const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer self.alloc.free(prefix);
        const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer if (upper) |buf| self.alloc.free(buf);

        const writes_end = self.writes.items.len;
        for (self.writes.items[0..writes_end]) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareCascadeForeignKeyChild(decoded.child_key);
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareCascadeForeignKeyChild(decoded.child_key);
        }
    }

    fn prepareCascadeForeignKeyChild(self: *WriteParticipant, child_key: []const u8) anyerror!void {
        if (containsKey(self.planned_delete_keys, child_key)) return;
        if (try self.isRowDeletePlanned(child_key)) return;
        if (self.cascade_depth >= max_cascade_depth) return error.ForeignKeyViolation;
        if (self.cascade_delete_count >= max_cascade_deletes) return error.ForeignKeyViolation;

        self.cascade_depth += 1;
        self.cascade_delete_count += 1;
        defer self.cascade_depth -= 1;
        try self.prepareDelete(self.effectiveTableName(), child_key, null);
    }

    fn deferForeignKeyParentCheck(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
    ) !void {
        const parent_key_owned = try self.alloc.dupe(u8, parent_key);
        var parent_key_transferred = false;
        errdefer if (!parent_key_transferred) self.alloc.free(parent_key_owned);
        try self.pending_fk_parent_checks.append(self.alloc, .{
            .foreign_key = foreign_key,
            .parent_key = parent_key_owned,
        });
        parent_key_transferred = true;
    }

    fn validatePendingForeignKeyParentChecks(self: *WriteParticipant) !void {
        for (self.pending_fk_parent_checks.items) |check| {
            try self.requireForeignKeyParentExists(check.foreign_key, check.parent_key);
        }
    }

    fn clearPendingForeignKeyParentChecks(self: *WriteParticipant) void {
        for (self.pending_fk_parent_checks.items) |check| {
            self.alloc.free(@constCast(check.parent_key));
        }
        self.pending_fk_parent_checks.deinit(self.alloc);
        self.pending_fk_parent_checks = .empty;
    }

    fn requireForeignKeyParentExists(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8) !void {
        if (!foreignKeyReferencesPrimaryKey(foreign_key)) {
            const unique_constraint = findUniqueConstraintByColumns(self.unique_constraints, foreign_key.parent_columns) orelse return error.ForeignKeyViolation;
            return try self.requireForeignKeyUniqueParentExists(unique_constraint, parent_key);
        }
        if (containsKey(self.planned_delete_keys, parent_key)) return error.ForeignKeyViolation;
        const row_key = try rowKeyAlloc(self.alloc, parent_key);
        defer self.alloc.free(row_key);
        if (containsBatchDelete(self.deletes.items, row_key)) return error.ForeignKeyViolation;
        if (containsBatchWrite(self.writes.items, row_key)) return;
        const raw = try getRawAlloc(self.alloc, self.store, parent_key);
        if (raw) |value| {
            self.alloc.free(value);
            return;
        }
        return error.ForeignKeyViolation;
    }

    fn requireForeignKeyUniqueParentExists(self: *WriteParticipant, constraint: schema_mod.UniqueConstraint, encoded_value: []const u8) !void {
        const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, constraint.name, encoded_value);
        defer self.alloc.free(key);
        var owner_owned = false;
        const owner = if (batchWriteValue(self.writes.items, key)) |value| value else blk: {
            if (containsBatchDelete(self.deletes.items, key)) return error.ForeignKeyViolation;
            const raw = self.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyViolation,
                else => return err,
            };
            owner_owned = true;
            break :blk raw;
        };
        defer if (owner_owned) self.alloc.free(owner);
        if (containsKey(self.planned_delete_keys, owner)) return error.ForeignKeyViolation;
        const row_key = try rowKeyAlloc(self.alloc, owner);
        defer self.alloc.free(row_key);
        if (containsBatchDelete(self.deletes.items, row_key)) return error.ForeignKeyViolation;
        if (containsBatchWrite(self.writes.items, row_key)) return;
        const raw = try getRawAlloc(self.alloc, self.store, owner);
        if (raw) |value| {
            self.alloc.free(value);
            return;
        }
        return error.ForeignKeyViolation;
    }

    fn requireNoRestrictingPrimaryKeyForeignKeyRefs(self: *WriteParticipant, parent_key: []const u8) !void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .restrict) continue;
            if (!foreignKeyReferencesPrimaryKey(foreign_key)) continue;
            try self.requireNoRestrictingForeignKeyRefsForIdentity(foreign_key, parent_key);
        }
    }

    fn requireNoRestrictingUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) !void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!stringSlicesEqual(foreign_key.parent_columns, constraint.columns)) continue;
            try self.requireNoRestrictingForeignKeyRefsForIdentity(foreign_key, encoded_value);
        }
    }

    fn requireNoRestrictingForeignKeyRefsForIdentity(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8) !void {
        const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer self.alloc.free(prefix);
        const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
        );
        defer if (upper) |buf| self.alloc.free(buf);

        for (self.writes.items) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (containsKey(self.planned_delete_keys, decoded.child_key)) continue;
            return error.ForeignKeyViolation;
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (containsKey(self.planned_delete_keys, decoded.child_key)) continue;
            return error.ForeignKeyViolation;
        }
    }

    fn appendForeignKeyRefWrite(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8, child_key: []const u8) !void {
        const key = try internal_keys.relationalForeignKeyRefKeyAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
            self.effectiveTableName(),
            child_key,
        );
        var key_owned = true;
        errdefer if (key_owned) self.alloc.free(key);
        try self.owned_keys.append(self.alloc, key);
        key_owned = false;
        try self.writes.append(self.alloc, .{ .key = key, .value = "" });
    }

    fn appendForeignKeyRefDelete(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8, child_key: []const u8) !void {
        const key = try internal_keys.relationalForeignKeyRefKeyAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            parent_key,
            self.effectiveTableName(),
            child_key,
        );
        var key_owned = true;
        errdefer if (key_owned) self.alloc.free(key);
        try self.owned_keys.append(self.alloc, key);
        key_owned = false;
        try self.deletes.append(self.alloc, key);
    }
};

fn foreignKeyReferencesPrimaryKey(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.parent_columns.len == 1 and std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
}

fn foreignKeyIsEnforced(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.validation_state == .enforced;
}

fn findUniqueConstraintByColumns(constraints: []const schema_mod.UniqueConstraint, columns: []const []const u8) ?schema_mod.UniqueConstraint {
    for (constraints) |constraint| {
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn foreignKeyParentExists(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    foreign_key: schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    parent_key: []const u8,
) !bool {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) {
        const parent = try getRawAlloc(alloc, store, parent_key);
        if (parent) |raw| {
            alloc.free(raw);
            return true;
        }
        return false;
    }

    const unique_constraint = findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return false;
    const unique_key = try internal_keys.relationalUniqueKeyAlloc(alloc, unique_constraint.name, parent_key);
    defer alloc.free(unique_key);
    const owner = store.get(alloc, unique_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    defer alloc.free(owner);
    const parent = try getRawAlloc(alloc, store, owner);
    if (parent) |raw| {
        alloc.free(raw);
        return true;
    }
    return false;
}

pub fn foreignKeyReferenceValueAlloc(alloc: Allocator, row_value: []const u8, foreign_key: schema_mod.ForeignKey) !?[]u8 {
    if (!foreignKeyReferencesPrimaryKey(foreign_key)) {
        return try uniqueConstraintColumnsTupleValueAlloc(alloc, row_value, foreign_key.child_columns);
    }
    return try foreignKeyPrimaryKeyValueAlloc(alloc, row_value, foreign_key.child_columns[0]);
}

fn foreignKeyPrimaryKeyValueAlloc(alloc: Allocator, row_value: []const u8, column_path: []const u8) !?[]u8 {
    const cell = (try relational_row_codec.findCellByPath(row_value, column_path)) orelse return null;
    if (cell.value_type != .bytes_val) return error.InvalidColumnValue;
    if (cell.value.bytes_val.len == 0) return null;
    return try alloc.dupe(u8, cell.value.bytes_val);
}

fn relationalRowWithoutColumnsAlloc(alloc: Allocator, row_value: []const u8, columns: []const []const u8) ![]u8 {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);

    var cells = std.ArrayListUnmanaged(relational_row_codec.Cell).empty;
    defer cells.deinit(alloc);
    for (row.cells) |cell| {
        if (containsKey(columns, cell.path)) continue;
        try cells.append(alloc, cell);
    }
    return try relational_row_codec.serialize(alloc, cells.items);
}

pub fn uniqueConstraintTupleValueAlloc(alloc: Allocator, row_value: []const u8, constraint: schema_mod.UniqueConstraint) !?[]u8 {
    return try uniqueConstraintColumnsTupleValueAlloc(alloc, row_value, constraint.columns);
}

fn uniqueConstraintColumnsTupleValueAlloc(alloc: Allocator, row_value: []const u8, columns: []const []const u8) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueAlloc(alloc, row_value, column_path)) orelse {
            out.deinit(alloc);
            return null;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    return try out.toOwnedSlice(alloc);
}

fn decodeForeignKeyParentTupleValuesAlloc(alloc: Allocator, foreign_key: schema_mod.ForeignKey, encoded_parent_key: []const u8) ![]ForeignKeyIntegrityTupleValue {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return try alloc.alloc(ForeignKeyIntegrityTupleValue, 0);

    var values = std.ArrayListUnmanaged(ForeignKeyIntegrityTupleValue).empty;
    errdefer {
        for (values.items) |*value| value.deinit(alloc);
        values.deinit(alloc);
    }

    var pos: usize = 0;
    for (foreign_key.parent_columns) |column| {
        const term = internal_keys.findComponentTerminator(encoded_parent_key, pos) orelse return error.InvalidColumnValue;
        const component = try internal_keys.decodeBodyAlloc(alloc, encoded_parent_key[pos..term]);
        defer alloc.free(component);

        const column_owned = try alloc.dupe(u8, column);
        var column_transferred = false;
        errdefer if (!column_transferred) alloc.free(column_owned);
        const value_owned = try decodeUniqueConstraintDisplayValueAlloc(alloc, component);
        var value_transferred = false;
        errdefer if (!value_transferred) alloc.free(value_owned);

        try values.append(alloc, .{
            .column = column_owned,
            .value = value_owned,
        });
        column_transferred = true;
        value_transferred = true;
        pos = term + 2;
    }
    if (pos != encoded_parent_key.len) return error.InvalidColumnValue;

    return try values.toOwnedSlice(alloc);
}

fn decodeUniqueConstraintDisplayValueAlloc(alloc: Allocator, component: []const u8) ![]u8 {
    if (component.len == 0) return error.InvalidColumnValue;
    const value_type = typedValueTypeFromByte(component[0]) orelse return error.InvalidColumnValue;
    const payload = component[1..];
    return switch (value_type) {
        .u64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            const value = std.mem.readInt(u64, payload[0..8], .big);
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{value});
        },
        .f64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            const value: f64 = @bitCast(std.mem.readInt(u64, payload[0..8], .big));
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{value});
        },
        .bool_val => blk: {
            if (payload.len != 1) return error.InvalidColumnValue;
            break :blk try alloc.dupe(u8, if (payload[0] == 0) "false" else "true");
        },
        .geo_point => blk: {
            if (payload.len != 16) return error.InvalidColumnValue;
            const lat: f64 = @bitCast(std.mem.readInt(u64, payload[0..8], .big));
            const lon: f64 = @bitCast(std.mem.readInt(u64, payload[8..16], .big));
            break :blk try std.fmt.allocPrint(alloc, "{{\"lat\":{d},\"lon\":{d}}}", .{ lat, lon });
        },
        .bytes_val => try alloc.dupe(u8, payload),
    };
}

fn typedValueTypeFromByte(tag: u8) ?typed_dv.ValueType {
    return switch (tag) {
        @intFromEnum(typed_dv.ValueType.u64_val) => .u64_val,
        @intFromEnum(typed_dv.ValueType.f64_val) => .f64_val,
        @intFromEnum(typed_dv.ValueType.bytes_val) => .bytes_val,
        @intFromEnum(typed_dv.ValueType.geo_point) => .geo_point,
        @intFromEnum(typed_dv.ValueType.bool_val) => .bool_val,
        else => null,
    };
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn uniqueConstraintColumnValueAlloc(alloc: Allocator, row_value: []const u8, column_path: []const u8) !?[]u8 {
    const cell = (try relational_row_codec.findCellByPath(row_value, column_path)) orelse return null;
    if (cell.is_json) return error.InvalidColumnValue;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, @intFromEnum(cell.value_type));
    switch (cell.value) {
        .u64_val => |value| {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, value, .big);
            try out.appendSlice(alloc, &buf);
        },
        .f64_val => |value| {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, @bitCast(value), .big);
            try out.appendSlice(alloc, &buf);
        },
        .bool_val => |value| try out.append(alloc, if (value) 1 else 0),
        .geo_point => |value| {
            var lat_buf: [8]u8 = undefined;
            var lon_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &lat_buf, @bitCast(value.lat), .big);
            std.mem.writeInt(u64, &lon_buf, @bitCast(value.lon), .big);
            try out.appendSlice(alloc, &lat_buf);
            try out.appendSlice(alloc, &lon_buf);
        },
        .bytes_val => |value| try out.appendSlice(alloc, value),
    }
    return try out.toOwnedSlice(alloc);
}

fn optionalBytesEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn containsKey(keys: []const []const u8, needle: []const u8) bool {
    for (keys) |key| {
        if (std.mem.eql(u8, key, needle)) return true;
    }
    return false;
}

fn containsBatchWrite(writes: []const docstore_mod.KVPair, key: []const u8) bool {
    for (writes) |write| {
        if (std.mem.eql(u8, write.key, key)) return true;
    }
    return false;
}

fn batchWriteValue(writes: []const docstore_mod.KVPair, key: []const u8) ?[]const u8 {
    for (writes) |write| {
        if (std.mem.eql(u8, write.key, key)) return write.value;
    }
    return null;
}

fn containsBatchDelete(deletes: []const []const u8, key: []const u8) bool {
    return containsKey(deletes, key);
}

fn countRelationalRowDeletes(deletes: []const []const u8) u64 {
    var count: u64 = 0;
    for (deletes) |key| {
        if (internal_keys.isRelationalRowKey(key)) count += 1;
    }
    return count;
}

fn classifyForeignKeyDeletePlanBlock(participant: WriteParticipant) ForeignKeyDeletePlanBlockReason {
    if (participant.set_null_update_count >= participant.set_null_update_limit) return .local_set_null_limit;
    if (participant.cascade_delete_count >= max_cascade_deletes or participant.cascade_depth >= max_cascade_depth) return .local_cascade_limit;
    return .restrict;
}

pub fn appendUpsert(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, owned_keys, owned_values, doc_key, row_value, ColumnIndexPolicy.all());
}

pub fn appendUpsertWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, owned_keys, owned_values, doc_key, row_value, column_index_policy);
}

pub fn appendDelete(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    try appendDeleteInternal(alloc, store, deletes, owned_keys, doc_key);
}

pub fn appendUpsertOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, null, null, doc_key, row_value, ColumnIndexPolicy.all());
}

pub fn appendUpsertOwnedBatchWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, null, null, doc_key, row_value, column_index_policy);
}

pub fn appendDeleteOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
) !void {
    try appendDeleteInternal(alloc, store, deletes, null, doc_key);
}

pub fn appendColumnIndexWritesForRow(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendColumnIndexWritesForRowWithColumnIndexPolicy(alloc, writes, owned_keys, owned_values, doc_key, row_value, ColumnIndexPolicy.all());
}

pub fn appendColumnIndexWritesForRowWithColumnIndexPolicy(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        if (!column_index_policy.shouldIndex(cell.path)) continue;
        try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, cell);
    }
}

pub fn appendColumnIndexDeletesForRow(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        try appendColumnIndexDeleteForCell(alloc, deletes, owned_keys, doc_key, cell);
    }
}

pub fn getRawAlloc(alloc: Allocator, store: *docstore_mod.DocStore, doc_key: []const u8) !?[]u8 {
    const key = try rowKeyAlloc(alloc, doc_key);
    defer alloc.free(key);
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn getMaterializedAlloc(alloc: Allocator, store: *docstore_mod.DocStore, doc_key: []const u8) !?[]u8 {
    const raw = try getRawAlloc(alloc, store, doc_key) orelse return null;
    defer alloc.free(raw);
    return try relational_row_codec.reconstructValueAlloc(alloc, raw);
}

pub fn freeRows(alloc: Allocator, rows: []OwnedRow) void {
    for (rows) |*row| row.deinit(alloc);
    alloc.free(rows);
}

pub fn freeColumnValues(alloc: Allocator, values: []OwnedColumnValue) void {
    for (values) |*value| value.deinit(alloc);
    alloc.free(values);
}

pub fn scanRowsAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]OwnedRow {
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower);
    const upper = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged(OwnedRow).empty;
    errdefer {
        for (out.items) |*row| row.deinit(alloc);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        if (!internal_keys.isRelationalRowKey(entry.key)) continue;
        const doc_key = (try internal_keys.decodeRelationalRowKeyAlloc(alloc, entry.key)) orelse continue;
        errdefer alloc.free(doc_key);
        const row_value = try alloc.dupe(u8, entry.value);
        errdefer alloc.free(row_value);
        try out.append(alloc, .{
            .doc_key = doc_key,
            .row_value = row_value,
        });
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanColumnAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]OwnedColumnValue {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalColumnIndexPrefixAlloc(alloc, column_path);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged(OwnedColumnValue).empty;
    errdefer {
        for (out.items) |*value| value.deinit(alloc);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalColumnIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const raw_row = try getRawAlloc(alloc, store, decoded.doc_key) orelse continue;
        defer alloc.free(raw_row);
        const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse continue;
        const doc_key = try alloc.dupe(u8, decoded.doc_key);
        var doc_key_owned = true;
        errdefer if (doc_key_owned) alloc.free(doc_key);
        const value = try cloneTypedValue(alloc, cell.value_type, cell.value);
        var value_owned = cell.value_type == .bytes_val;
        errdefer if (value_owned) alloc.free(value.bytes_val);
        try out.append(alloc, .{
            .doc_key = doc_key,
            .value_type = cell.value_type,
            .is_json = cell.is_json,
            .value = value,
        });
        doc_key_owned = false;
        value_owned = false;
    }

    return try out.toOwnedSlice(alloc);
}

pub fn rebuildAllColumnIndexesFromRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    try rebuildAllColumnIndexesFromRowsInRangeWithColumnIndexPolicy(alloc, store, lower_doc_key, upper_doc_key, ColumnIndexPolicy.all());
}

pub fn rebuildAllColumnIndexesFromRowsInRangeWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    // This is intentionally a whole-secondary-namespace replacement: split
    // finalization and destination build use it after the physical row set has
    // already been reduced to the target range.
    try clearColumnIndexNamespace(alloc, store);

    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    for (rows) |row| {
        try appendColumnIndexWritesForRowWithColumnIndexPolicy(
            alloc,
            &writes,
            &owned_keys,
            &owned_values,
            row.doc_key,
            row.row_value,
            column_index_policy,
        );
    }

    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
}

pub fn pruneColumnIndexesForMissingRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_column_index_namespace};
    const upper = [_]u8{internal_keys.relational_column_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalColumnIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const row_exists = try getRawAlloc(alloc, store, decoded.doc_key);
        if (row_exists) |raw| {
            alloc.free(raw);
        } else {
            try deletes.append(alloc, entry.key);
        }
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

pub fn deleteColumnIndexesByDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower = try internal_keys.relationalColumnIndexByDocRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower);
    const upper = try internal_keys.relationalColumnIndexByDocRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalColumnIndexByDocKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);

        try deletes.append(alloc, entry.key);
        const column_major = try internal_keys.relationalColumnIndexKeyAlloc(alloc, decoded.column_path, decoded.doc_key);
        var column_major_owned = true;
        errdefer if (column_major_owned) alloc.free(column_major);
        try owned_keys.append(alloc, column_major);
        column_major_owned = false;
        try deletes.append(alloc, column_major);
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

pub fn deleteColumnIndexesForRowRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }

    for (rows) |row| {
        try appendColumnIndexDeletesForRow(alloc, &deletes, &owned_keys, row.doc_key, row.row_value);
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

pub fn rebuildUniqueConstraintRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    if (unique_constraints.len == 0) return;

    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    for (rows) |row| {
        for (unique_constraints) |constraint| {
            const encoded_value = (try uniqueConstraintTupleValueAlloc(alloc, row.row_value, constraint)) orelse continue;
            defer alloc.free(encoded_value);

            const key = try internal_keys.relationalUniqueKeyAlloc(alloc, constraint.name, encoded_value);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);

            if (batchWriteValue(writes.items, key)) |existing_owner| {
                if (!std.mem.eql(u8, existing_owner, row.doc_key)) return error.UniqueConstraintViolation;
                alloc.free(key);
                continue;
            }
            if (store.get(alloc, key)) |stored_owner| {
                defer alloc.free(stored_owner);
                if (!std.mem.eql(u8, stored_owner, row.doc_key)) return error.UniqueConstraintViolation;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const owner_value = try alloc.dupe(u8, row.doc_key);
            var owner_value_owned = true;
            errdefer if (owner_value_owned) alloc.free(owner_value);
            try owned_keys.append(alloc, key);
            key_owned = false;
            try owned_values.append(alloc, owner_value);
            owner_value_owned = false;
            try writes.append(alloc, .{ .key = key, .value = owner_value });
        }
    }

    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
}

pub fn deleteUniqueConstraintRows(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    unique_constraints: []const schema_mod.UniqueConstraint,
) !void {
    if (unique_constraints.len == 0) return;
    const lower = [_]u8{internal_keys.relational_unique_namespace};
    const upper = [_]u8{internal_keys.relational_unique_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (scanned) |entry| {
        const constraint_name = (try decodeRelationalUniqueConstraintNameAlloc(alloc, entry.key)) orelse continue;
        defer alloc.free(constraint_name);
        if (!uniqueConstraintNameInCatalog(unique_constraints, constraint_name)) continue;
        try deletes.append(alloc, entry.key);
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

pub fn deleteForeignKeyRefRows(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    foreign_keys: []const schema_mod.ForeignKey,
) !void {
    if (foreign_keys.len == 0) return;
    const lower = [_]u8{internal_keys.relational_foreign_key_ref_namespace};
    const upper = [_]u8{internal_keys.relational_foreign_key_ref_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!foreignKeyNameInCatalog(foreign_keys, decoded.constraint_name)) continue;
        try deletes.append(alloc, entry.key);
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

pub fn validateForeignKeyRefsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRange(alloc, store, table_name, foreign_keys, unique_constraints, lower_doc_key, upper_doc_key, .validate);
}

pub fn repairForeignKeyRefsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRange(alloc, store, table_name, foreign_keys, unique_constraints, lower_doc_key, upper_doc_key, .repair);
}

pub fn dryRunRepairForeignKeyRefsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRange(alloc, store, table_name, foreign_keys, unique_constraints, lower_doc_key, upper_doc_key, .dry_run);
}

pub fn reconcileForeignKeyRefOwnerForParent(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    mode: ForeignKeyIntegrityMode,
) !ForeignKeyIntegrityReport {
    var report: ForeignKeyIntegrityReport = .{};
    const foreign_key = findForeignKeyByName(foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
    if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }

    const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, constraint_name, parent_table, parent_key);
    defer alloc.free(prefix);
    const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, constraint_name, parent_table, parent_key);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.child_table, table_name)) continue;
        report.scanned_ref_rows += 1;

        var stale = false;
        const child_row = try getRawAlloc(alloc, store, decoded.child_key);
        if (child_row) |raw| {
            defer alloc.free(raw);
            if (try foreignKeyReferenceValueAlloc(alloc, raw, foreign_key)) |current_parent| {
                defer alloc.free(current_parent);
                stale = !std.mem.eql(u8, current_parent, decoded.parent_key);
            } else {
                stale = true;
            }
        } else {
            stale = true;
        }
        if (!stale) continue;

        report.stale_ref_rows += 1;
        if (mode == .repair or mode == .dry_run) {
            report.deleted_stale_ref_rows += 1;
        }
        if (mode == .repair) {
            const key = try alloc.dupe(u8, entry.key);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            try owned_keys.append(alloc, key);
            key_owned = false;
            try deletes.append(alloc, key);
        }
    }

    if (mode == .repair and deletes.items.len > 0) {
        try store.putBatch(&.{}, deletes.items);
    }
    return report;
}

pub fn explainForeignKeyDelete(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    doc_key: []const u8,
) !ForeignKeyDeletePlan {
    var plan = ForeignKeyDeletePlan{};
    const old_row = try getRawAlloc(alloc, store, doc_key);
    if (old_row) |row| {
        alloc.free(row);
        plan.exists = true;
    } else {
        return plan;
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer owned_keys.deinit(alloc);
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer owned_values.deinit(alloc);

    var participant = WriteParticipant.init(alloc, store, &writes, &deletes, &owned_keys, &owned_values);
    participant.configureForeignKeys(table_name, foreign_keys, &.{doc_key}, false);
    participant.configureUniqueConstraints(unique_constraints);
    const prepared = participant.prepareDelete(table_name, doc_key, null);
    if (prepared) |_| {
        plan.allowed = true;
    } else |err| switch (err) {
        error.ForeignKeyViolation => {
            plan.allowed = false;
            plan.block_reason = classifyForeignKeyDeletePlanBlock(participant);
        },
        else => {
            participant.abort(null);
            return err;
        },
    }

    plan.planned_set_null_updates = participant.set_null_update_count;
    plan.planned_cascade_deletes = participant.cascade_delete_count;
    plan.planned_writes = @intCast(writes.items.len);
    plan.planned_row_deletes = countRelationalRowDeletes(deletes.items);
    plan.planned_index_deletes = @as(u64, @intCast(deletes.items.len)) - plan.planned_row_deletes;
    participant.abort(null);
    return plan;
}

pub fn listForeignKeyViolationsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]ForeignKeyIntegrityViolation {
    var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
    errdefer {
        for (violations.items) |*violation| violation.deinit(alloc);
        violations.deinit(alloc);
    }
    _ = try reconcileForeignKeyRefsInRangeWithViolations(
        alloc,
        store,
        table_name,
        foreign_keys,
        unique_constraints,
        lower_doc_key,
        upper_doc_key,
        .validate,
        &violations,
    );
    return try violations.toOwnedSlice(alloc);
}

pub fn reconcileForeignKeyRefsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRangeWithViolations(alloc, store, table_name, foreign_keys, unique_constraints, lower_doc_key, upper_doc_key, mode, null);
}

fn reconcileForeignKeyRefsInRangeWithViolations(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
    violations: ?*std.ArrayListUnmanaged(ForeignKeyIntegrityViolation),
) !ForeignKeyIntegrityReport {
    var report: ForeignKeyIntegrityReport = .{};
    if (foreign_keys.len == 0) return report;

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }

    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    for (rows) |row| {
        report.scanned_child_rows += 1;
        for (foreign_keys) |foreign_key| {
            const parent_key = (try foreignKeyReferenceValueAlloc(alloc, row.row_value, foreign_key)) orelse continue;
            defer alloc.free(parent_key);
            report.referenced_child_rows += 1;

            if (!(try foreignKeyParentExists(alloc, store, foreign_key, unique_constraints, parent_key))) {
                report.missing_parent_rows += 1;
                if (violations) |out| {
                    try appendForeignKeyIntegrityViolation(
                        alloc,
                        out,
                        .missing_parent,
                        foreign_key,
                        table_name,
                        row.doc_key,
                        parent_key,
                        null,
                    );
                }
            }

            const ref_key = try internal_keys.relationalForeignKeyRefKeyAlloc(
                alloc,
                foreign_key.name,
                foreign_key.parent_table,
                parent_key,
                table_name,
                row.doc_key,
            );
            var ref_key_owned = true;
            errdefer if (ref_key_owned) alloc.free(ref_key);
            if (store.get(alloc, ref_key)) |value| {
                alloc.free(value);
            } else |err| switch (err) {
                error.NotFound => {
                    report.missing_ref_rows += 1;
                    if (violations) |out| {
                        try appendForeignKeyIntegrityViolation(
                            alloc,
                            out,
                            .missing_ref,
                            foreign_key,
                            table_name,
                            row.doc_key,
                            parent_key,
                            null,
                        );
                    }
                    if (mode == .repair or mode == .dry_run) {
                        report.repaired_ref_rows += 1;
                    }
                    if (mode == .repair) {
                        try owned_keys.append(alloc, ref_key);
                        ref_key_owned = false;
                        try writes.append(alloc, .{ .key = ref_key, .value = "" });
                    }
                },
                else => return err,
            }
            if (ref_key_owned) alloc.free(ref_key);
        }
    }

    try pruneStaleForeignKeyRefsInRange(
        alloc,
        store,
        table_name,
        foreign_keys,
        lower_doc_key,
        upper_doc_key,
        mode,
        &report,
        &deletes,
        &owned_keys,
        violations,
    );

    if (mode == .repair and (writes.items.len > 0 or deletes.items.len > 0)) {
        try store.putBatch(writes.items, deletes.items);
    }
    return report;
}

fn pruneStaleForeignKeyRefsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
    report: *ForeignKeyIntegrityReport,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    violations: ?*std.ArrayListUnmanaged(ForeignKeyIntegrityViolation),
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_foreign_key_ref_namespace};
    const upper = [_]u8{internal_keys.relational_foreign_key_ref_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        const foreign_key = findForeignKeyByName(foreign_keys, decoded.constraint_name) orelse continue;
        if (!std.mem.eql(u8, decoded.child_table, table_name)) continue;
        if (!std.mem.eql(u8, decoded.parent_table, foreign_key.parent_table)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.child_key, lower_doc_bound, upper_doc_bound))) continue;
        report.scanned_ref_rows += 1;

        var stale = false;
        var observed_parent_key: ?[]u8 = null;
        defer if (observed_parent_key) |value| alloc.free(value);
        const child_row = try getRawAlloc(alloc, store, decoded.child_key);
        if (child_row) |raw| {
            defer alloc.free(raw);
            if (try foreignKeyReferenceValueAlloc(alloc, raw, foreign_key)) |current_parent| {
                stale = !std.mem.eql(u8, current_parent, decoded.parent_key);
                if (stale) observed_parent_key = current_parent;
                if (!stale) alloc.free(current_parent);
            } else {
                stale = true;
            }
        } else {
            stale = true;
        }
        if (!stale) continue;

        report.stale_ref_rows += 1;
        if (violations) |out| {
            try appendForeignKeyIntegrityViolation(
                alloc,
                out,
                .stale_ref,
                foreign_key,
                table_name,
                decoded.child_key,
                decoded.parent_key,
                observed_parent_key,
            );
        }
        if (mode == .repair or mode == .dry_run) {
            report.deleted_stale_ref_rows += 1;
        }
        if (mode == .repair) {
            const key = try alloc.dupe(u8, entry.key);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            try owned_keys.append(alloc, key);
            key_owned = false;
            try deletes.append(alloc, key);
        }
    }
}

fn appendForeignKeyIntegrityViolation(
    alloc: Allocator,
    violations: *std.ArrayListUnmanaged(ForeignKeyIntegrityViolation),
    kind: ForeignKeyIntegrityViolationKind,
    foreign_key: schema_mod.ForeignKey,
    child_table: []const u8,
    child_key: []const u8,
    parent_key: []const u8,
    observed_parent_key: ?[]const u8,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const child_table_owned = try alloc.dupe(u8, child_table);
    errdefer alloc.free(child_table_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const parent_values = try decodeForeignKeyParentTupleValuesAlloc(alloc, foreign_key, parent_key);
    errdefer freeForeignKeyIntegrityTupleValues(alloc, parent_values);
    const observed_parent_key_owned = if (observed_parent_key) |observed| try alloc.dupe(u8, observed) else null;
    errdefer if (observed_parent_key_owned) |observed| alloc.free(observed);
    const observed_parent_values = if (observed_parent_key) |observed|
        try decodeForeignKeyParentTupleValuesAlloc(alloc, foreign_key, observed)
    else
        try alloc.alloc(ForeignKeyIntegrityTupleValue, 0);
    errdefer freeForeignKeyIntegrityTupleValues(alloc, observed_parent_values);

    const violation = ForeignKeyIntegrityViolation{
        .kind = kind,
        .constraint_name = constraint_name,
        .child_table = child_table_owned,
        .child_key = child_key_owned,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .parent_values = parent_values,
        .observed_parent_key = observed_parent_key_owned,
        .observed_parent_values = observed_parent_values,
    };
    try violations.append(alloc, violation);
}

fn findForeignKeyByName(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) ?schema_mod.ForeignKey {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
    }
    return null;
}

fn decodeRelationalUniqueConstraintNameAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!internal_keys.isRelationalUniqueKey(key)) return null;
    const constraint_term = internal_keys.findComponentTerminator(key, 1) orelse return error.InvalidInternalUserKey;
    return try internal_keys.decodeBodyAlloc(alloc, key[1..constraint_term]);
}

fn uniqueConstraintNameInCatalog(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) bool {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return true;
    }
    return false;
}

fn foreignKeyNameInCatalog(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) bool {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return true;
    }
    return false;
}

fn clearColumnIndexNamespace(alloc: Allocator, store: *docstore_mod.DocStore) !void {
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_column_index_namespace);
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_column_index_by_doc_namespace);
}

fn clearColumnIndexNamespacePrefix(alloc: Allocator, store: *docstore_mod.DocStore, namespace: u8) !void {
    const lower = [_]u8{namespace};
    const upper = [_]u8{namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);
    if (scanned.len == 0) return;

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    try deletes.ensureUnusedCapacity(alloc, scanned.len);
    for (scanned) |entry| {
        if (!internal_keys.isRelationalColumnIndexKey(entry.key) and !internal_keys.isRelationalColumnIndexByDocKey(entry.key)) continue;
        deletes.appendAssumeCapacity(entry.key);
    }
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

fn docKeyFallsInRange(alloc: Allocator, doc_key: []const u8, lower: []const u8, upper: ?[]const u8) !bool {
    const encoded = try internal_keys.documentRangeLowerAlloc(alloc, doc_key);
    defer alloc.free(encoded);
    if (std.mem.order(u8, encoded, lower) == .lt) return false;
    if (upper) |upper_bound| {
        if (std.mem.order(u8, encoded, upper_bound) != .lt) return false;
    }
    return true;
}

fn nextPrefixAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
    if (prefix.len == 0) return null;
    var out = try alloc.dupe(u8, prefix);
    var i = out.len;
    while (i > 0) {
        i -= 1;
        if (out[i] != 0xff) {
            out[i] += 1;
            return try alloc.realloc(out, i + 1);
        }
    }
    alloc.free(out);
    return null;
}

fn appendUpsertInternal(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendExistingColumnDeletes(alloc, store, deletes, owned_keys, doc_key);

    const row_key = try rowKeyAlloc(alloc, doc_key);
    var row_key_owned = true;
    errdefer if (row_key_owned) alloc.free(row_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, row_key);
        row_key_owned = false;
    }
    try writes.append(alloc, .{
        .key = row_key,
        .value = row_value,
    });
    row_key_owned = false;

    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        const key = try internal_keys.relationalColumnKeyAlloc(alloc, doc_key, cell.path);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }

        const value = try relational_row_codec.serialize(alloc, &.{cell});
        var value_owned = true;
        errdefer if (value_owned) alloc.free(value);
        if (owned_values) |values| {
            try values.append(alloc, value);
            value_owned = false;
        }

        try writes.append(alloc, .{
            .key = key,
            .value = value,
        });
        key_owned = false;
        value_owned = false;

        if (column_index_policy.shouldIndex(cell.path)) {
            try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, cell);
        }
    }
}

fn appendColumnIndexWriteForCell(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    const index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, cell.path, doc_key);
    var index_key_owned = true;
    errdefer if (index_key_owned) alloc.free(index_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, index_key);
        index_key_owned = false;
    }

    const index_value = try relational_row_codec.serialize(alloc, &.{cell});
    var index_value_owned = true;
    errdefer if (index_value_owned) alloc.free(index_value);
    if (owned_values) |values| {
        try values.append(alloc, index_value);
        index_value_owned = false;
    }

    try writes.append(alloc, .{
        .key = index_key,
        .value = index_value,
    });
    index_key_owned = false;
    index_value_owned = false;

    const by_doc_key = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, doc_key, cell.path);
    var by_doc_key_owned = true;
    errdefer if (by_doc_key_owned) alloc.free(by_doc_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, by_doc_key);
        by_doc_key_owned = false;
    }

    try writes.append(alloc, .{
        .key = by_doc_key,
        .value = "",
    });
    by_doc_key_owned = false;
}

fn appendColumnIndexDeleteForCell(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    const index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, cell.path, doc_key);
    var index_key_owned = true;
    errdefer if (index_key_owned) alloc.free(index_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, index_key);
        index_key_owned = false;
    }
    try deletes.append(alloc, index_key);
    index_key_owned = false;

    const by_doc_key = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, doc_key, cell.path);
    var by_doc_key_owned = true;
    errdefer if (by_doc_key_owned) alloc.free(by_doc_key);
    if (owned_keys) |keys| {
        try keys.append(alloc, by_doc_key);
        by_doc_key_owned = false;
    }
    try deletes.append(alloc, by_doc_key);
    by_doc_key_owned = false;
}

fn appendDeleteInternal(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    try appendExistingColumnDeletes(alloc, store, deletes, owned_keys, doc_key);

    const key = try rowKeyAlloc(alloc, doc_key);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (owned_keys) |keys| {
        try keys.append(alloc, key);
        key_owned = false;
    }
    try deletes.append(alloc, key);
    key_owned = false;
}

fn appendExistingColumnDeletes(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
) !void {
    const old_row = try getRawAlloc(alloc, store, doc_key) orelse return;
    defer alloc.free(old_row);
    var row = try relational_row_codec.deserialize(alloc, old_row);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        const key = try internal_keys.relationalColumnKeyAlloc(alloc, doc_key, cell.path);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try deletes.append(alloc, key);
        key_owned = false;

        try appendColumnIndexDeleteForCell(alloc, deletes, owned_keys, doc_key, cell);
    }
}

fn cloneTypedValue(alloc: Allocator, value_type: typed_dv.ValueType, value: typed_dv.TypedValue) !typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => .{ .u64_val = value.u64_val },
        .f64_val => .{ .f64_val = value.f64_val },
        .bool_val => .{ .bool_val = value.bool_val },
        .geo_point => .{ .geo_point = value.geo_point },
        .bytes_val => .{ .bytes_val = try alloc.dupe(u8, value.bytes_val) },
    };
}

test "relational base store writes materialize and delete by document key" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cells = [_]relational_row_codec.Cell{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alpha" },
        },
    };
    const row = try relational_row_codec.serialize(alloc, &cells);
    defer alloc.free(row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row);
    try store.putBatch(writes.items, deletes.items);

    const materialized = (try getMaterializedAlloc(alloc, &store, "doc:a")).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", materialized);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expect((try getRawAlloc(alloc, &store, "doc:a")) == null);
}

test "relational write participant prepares commit and abort boundaries" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "prepared" },
        },
    });
    defer alloc.free(row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    var participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try participant.prepareUpsert("events", "doc:a", row, null);
    participant.abort(null);
    const aborted_raw = try getRawAlloc(alloc, &store, "doc:a");
    defer if (aborted_raw) |value| alloc.free(value);
    try std.testing.expect(aborted_raw == null);

    var committed = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try committed.prepareUpsert("events", "doc:a", row, null);
    try committed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const materialized = (try committed.get("doc:a", null)).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"title\":\"prepared\"}", materialized);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    try delete_participant.prepareDelete("events", "doc:a", null);
    try delete_participant.commit(null, 2);
    try store.putBatch(writes.items, deletes.items);
    const deleted_raw = try getRawAlloc(alloc, &store, "doc:a");
    defer if (deleted_raw) |value| alloc.free(value);
    try std.testing.expect(deleted_raw == null);
}

test "relational write participant rejects set null fanout beyond local limit" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .set_null,
    }};
    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:wide" },
        },
    });
    defer alloc.free(parent_row);
    const child_one_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:wide:1" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:wide" },
        },
    });
    defer alloc.free(child_one_row);
    const child_two_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:wide:2" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:wide" },
        },
    });
    defer alloc.free(child_two_row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    var create_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{}, false);
    try create_participant.prepareUpsert("row", "customer:wide", parent_row, null);
    try create_participant.prepareUpsert("row", "order:wide:1", child_one_row, null);
    try create_participant.prepareUpsert("row", "order:wide:2", child_two_row, null);
    try create_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    delete_participant.configureForeignKeys("row", foreign_keys[0..], &.{"customer:wide"}, false);
    delete_participant.set_null_update_limit = 1;
    try std.testing.expectError(error.ForeignKeyViolation, delete_participant.prepareDelete("row", "customer:wide", null));
    delete_participant.abort(null);

    const parent = (try getMaterializedAlloc(alloc, &store, "customer:wide")) orelse return error.TestExpectedEqual;
    defer alloc.free(parent);
    const child_one = (try getMaterializedAlloc(alloc, &store, "order:wide:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_one);
    try std.testing.expect(std.mem.indexOf(u8, child_one, "\"customer_id\":\"customer:wide\"") != null);
}

test "relational foreign key delete explain plans set null without mutating rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .set_null,
    }};
    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:explain" },
        },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:explain" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:explain" },
        },
    });
    defer alloc.free(child_row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    var create_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{}, false);
    try create_participant.prepareUpsert("row", "customer:explain", parent_row, null);
    try create_participant.prepareUpsert("row", "order:explain", child_row, null);
    try create_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const plan = try explainForeignKeyDelete(alloc, &store, "row", foreign_keys[0..], &.{}, "customer:explain");
    try std.testing.expect(plan.exists);
    try std.testing.expect(plan.allowed);
    try std.testing.expectEqual(ForeignKeyDeletePlanBlockReason.none, plan.block_reason);
    try std.testing.expectEqual(@as(u64, 1), plan.planned_set_null_updates);
    try std.testing.expectEqual(@as(u64, 0), plan.planned_cascade_deletes);
    try std.testing.expectEqual(@as(u64, 1), plan.planned_row_deletes);
    try std.testing.expect(plan.planned_writes > 0);
    try std.testing.expect(plan.touchesChildren());

    const child_after = (try getMaterializedAlloc(alloc, &store, "order:explain")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_after);
    try std.testing.expect(std.mem.indexOf(u8, child_after, "\"customer_id\":\"customer:explain\"") != null);
}

test "relational foreign key delete explain plans cascade without mutating rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .cascade,
    }};
    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:cascade" },
        },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:cascade" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:cascade" },
        },
    });
    defer alloc.free(child_row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    var create_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{}, false);
    try create_participant.prepareUpsert("row", "customer:cascade", parent_row, null);
    try create_participant.prepareUpsert("row", "order:cascade", child_row, null);
    try create_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const plan = try explainForeignKeyDelete(alloc, &store, "row", foreign_keys[0..], &.{}, "customer:cascade");
    try std.testing.expect(plan.exists);
    try std.testing.expect(plan.allowed);
    try std.testing.expectEqual(ForeignKeyDeletePlanBlockReason.none, plan.block_reason);
    try std.testing.expectEqual(@as(u64, 0), plan.planned_set_null_updates);
    try std.testing.expectEqual(@as(u64, 1), plan.planned_cascade_deletes);
    try std.testing.expectEqual(@as(u64, 2), plan.planned_row_deletes);
    try std.testing.expect(plan.touchesChildren());

    const child_after = (try getMaterializedAlloc(alloc, &store, "order:cascade")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_after);
    try std.testing.expect(std.mem.indexOf(u8, child_after, "\"customer_id\":\"customer:cascade\"") != null);
}

test "relational foreign key delete explain reports restrict block" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
    }};
    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:restrict" },
        },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:restrict" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:restrict" },
        },
    });
    defer alloc.free(child_row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    var create_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{}, false);
    try create_participant.prepareUpsert("row", "customer:restrict", parent_row, null);
    try create_participant.prepareUpsert("row", "order:restrict", child_row, null);
    try create_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const plan = try explainForeignKeyDelete(alloc, &store, "row", foreign_keys[0..], &.{}, "customer:restrict");
    try std.testing.expect(plan.exists);
    try std.testing.expect(!plan.allowed);
    try std.testing.expectEqual(ForeignKeyDeletePlanBlockReason.restrict, plan.block_reason);
    try std.testing.expectEqual(@as(u64, 0), plan.planned_set_null_updates);
    try std.testing.expectEqual(@as(u64, 0), plan.planned_cascade_deletes);

    const parent_after = (try getMaterializedAlloc(alloc, &store, "customer:restrict")) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_after);
    try std.testing.expect(std.mem.indexOf(u8, parent_after, "customer:restrict") != null);
}

test "relational foreign key repair rebuilds missing refs and prunes stale refs" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:a" },
        },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "order:1" },
        },
        .{
            .path = "customer_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "customer:a" },
        },
    });
    defer alloc.free(child_row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "customer:a", parent_row);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "order:1", child_row);
    try store.putBatch(writes.items, deletes.items);

    var missing_report = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!missing_report.valid());
    try std.testing.expectEqual(@as(u64, 2), missing_report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 1), missing_report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 1), missing_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), missing_report.missing_parent_rows);

    const missing_violations = try listForeignKeyViolationsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    defer freeForeignKeyIntegrityViolations(alloc, missing_violations);
    try std.testing.expectEqual(@as(usize, 1), missing_violations.len);
    try std.testing.expectEqual(ForeignKeyIntegrityViolationKind.missing_ref, missing_violations[0].kind);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", missing_violations[0].constraint_name);
    try std.testing.expectEqualStrings("row", missing_violations[0].child_table);
    try std.testing.expectEqualStrings("order:1", missing_violations[0].child_key);
    try std.testing.expectEqualStrings("customers", missing_violations[0].parent_table);
    try std.testing.expectEqualStrings("customer:a", missing_violations[0].parent_key);
    try std.testing.expect(missing_violations[0].observed_parent_key == null);

    const dry_run_missing = try dryRunRepairForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!dry_run_missing.valid());
    try std.testing.expectEqual(@as(u64, 1), dry_run_missing.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), dry_run_missing.repaired_ref_rows);

    const after_dry_run_missing = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!after_dry_run_missing.valid());
    try std.testing.expectEqual(@as(u64, 1), after_dry_run_missing.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), after_dry_run_missing.repaired_ref_rows);

    const repaired_missing = try repairForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expectEqual(@as(u64, 1), repaired_missing.repaired_ref_rows);

    const repaired_report = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(repaired_report.valid());
    try std.testing.expectEqual(@as(u64, 1), repaired_report.scanned_ref_rows);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "order:1");
    try store.putBatch(writes.items, deletes.items);

    const stale_report = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!stale_report.valid());
    try std.testing.expectEqual(@as(u64, 1), stale_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), stale_report.stale_ref_rows);

    const stale_violations = try listForeignKeyViolationsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    defer freeForeignKeyIntegrityViolations(alloc, stale_violations);
    try std.testing.expectEqual(@as(usize, 1), stale_violations.len);
    try std.testing.expectEqual(ForeignKeyIntegrityViolationKind.stale_ref, stale_violations[0].kind);
    try std.testing.expectEqualStrings("order:1", stale_violations[0].child_key);
    try std.testing.expectEqualStrings("customer:a", stale_violations[0].parent_key);
    try std.testing.expect(stale_violations[0].observed_parent_key == null);

    const dry_run_stale = try dryRunRepairForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!dry_run_stale.valid());
    try std.testing.expectEqual(@as(u64, 1), dry_run_stale.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), dry_run_stale.deleted_stale_ref_rows);

    const after_dry_run_stale = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(!after_dry_run_stale.valid());
    try std.testing.expectEqual(@as(u64, 1), after_dry_run_stale.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), after_dry_run_stale.deleted_stale_ref_rows);

    const repaired_stale = try repairForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expectEqual(@as(u64, 1), repaired_stale.deleted_stale_ref_rows);

    const final_report = try validateForeignKeyRefsInRange(alloc, &store, "row", foreign_keys[0..], &.{}, "", "");
    try std.testing.expect(final_report.valid());
    try std.testing.expectEqual(@as(u64, 0), final_report.scanned_ref_rows);
}

test "relational base store scans rows and columns by document range" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alpha" },
        },
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 10.5 },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "beta" },
        },
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 20.25 },
        },
    });
    defer alloc.free(row_b);
    const row_c = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "gamma" },
        },
    });
    defer alloc.free(row_c);

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "doc:b");
    defer alloc.free(primary_key);
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:c", row_c);
    try writes.append(alloc, .{ .key = primary_key, .value = "{\"ignored\":true}" });
    try store.putBatch(writes.items, deletes.items);

    const rows = try scanRowsAlloc(alloc, &store, "doc:a", "doc:b");
    defer freeRows(alloc, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("doc:a", rows[0].doc_key);
    try std.testing.expectEqualStrings("doc:b", rows[1].doc_key);

    const amounts = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:b");
    defer freeColumnValues(alloc, amounts);
    try std.testing.expectEqual(@as(usize, 2), amounts.len);
    try std.testing.expectEqualStrings("doc:a", amounts[0].doc_key);
    try std.testing.expectEqual(@as(f64, 10.5), amounts[0].value.f64_val);
    try std.testing.expectEqualStrings("doc:b", amounts[1].doc_key);
    try std.testing.expectEqual(@as(f64, 20.25), amounts[1].value.f64_val);

    const doc_b_amount_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:b", "amount");
    defer alloc.free(doc_b_amount_key);
    const doc_b_amount = try store.get(alloc, doc_b_amount_key);
    defer alloc.free(doc_b_amount);
    const doc_b_amount_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(doc_b_amount_index_key);
    const doc_b_amount_index = try store.get(alloc, doc_b_amount_index_key);
    defer alloc.free(doc_b_amount_index);

    const row_b_without_amount = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "beta-updated" },
        },
    });
    defer alloc.free(row_b_without_amount);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b_without_amount);
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_amount_key));
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_amount_index_key));

    const amounts_after_update = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:c");
    defer freeColumnValues(alloc, amounts_after_update);
    try std.testing.expectEqual(@as(usize, 1), amounts_after_update.len);
    try std.testing.expectEqualStrings("doc:a", amounts_after_update[0].doc_key);

    const doc_a_amount_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "amount");
    defer alloc.free(doc_a_amount_key);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_a_amount_key));
}

test "relational column scans hydrate values from current base rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const old_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 10 },
        },
    });
    defer alloc.free(old_row);
    const current_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 20 },
        },
    });
    defer alloc.free(current_row);
    const row_without_amount = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "no amount" },
        },
    });
    defer alloc.free(row_without_amount);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", old_row);
    try store.putBatch(writes.items, deletes.items);

    const row_key = try rowKeyAlloc(alloc, "doc:a");
    defer alloc.free(row_key);
    try store.putBatch(&.{.{ .key = row_key, .value = current_row }}, &.{});

    const current_values = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:a");
    defer freeColumnValues(alloc, current_values);
    try std.testing.expectEqual(@as(usize, 1), current_values.len);
    try std.testing.expectEqualStrings("doc:a", current_values[0].doc_key);
    try std.testing.expectEqual(.f64_val, current_values[0].value_type);
    try std.testing.expectEqual(@as(f64, 20), current_values[0].value.f64_val);

    try store.putBatch(&.{.{ .key = row_key, .value = row_without_amount }}, &.{});

    const missing_values = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:a");
    defer freeColumnValues(alloc, missing_values);
    try std.testing.expectEqual(@as(usize, 0), missing_values.len);
}

test "relational column indexing policy preserves cells but suppresses scan entries" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 42.0 },
        },
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "kept" },
        },
    });
    defer alloc.free(row);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false, .indexed = false },
        .{ .name = "title", .path = "title", .field_type = .text, .nullable = true, .indexed = true },
    };
    try appendUpsertWithColumnIndexPolicy(
        alloc,
        &store,
        &writes,
        &deletes,
        &owned_keys,
        &owned_values,
        "doc:a",
        row,
        ColumnIndexPolicy.fromColumns(columns[0..]),
    );
    try store.putBatch(writes.items, deletes.items);

    const amount_cell_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "amount");
    defer alloc.free(amount_cell_key);
    const amount_cell = try store.get(alloc, amount_cell_key);
    defer alloc.free(amount_cell);

    const amount_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(amount_index_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, amount_index_key));

    const title_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "title", "doc:a");
    defer alloc.free(title_index_key);
    const title_index = try store.get(alloc, title_index_key);
    defer alloc.free(title_index);
}

test "relational column scan indexes rebuild and delete from packed rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 1.0 },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 2.0 },
        },
    });
    defer alloc.free(row_b);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b);
    try store.putBatch(writes.items, deletes.items);

    const doc_a_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(doc_a_index);
    const doc_b_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(doc_b_index);

    try store.putBatch(&.{}, &.{ doc_a_index, doc_b_index });
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_a_index));
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_index));

    try rebuildAllColumnIndexesFromRowsInRange(alloc, &store, "doc:a", "doc:c");
    const rebuilt_a = try store.get(alloc, doc_a_index);
    defer alloc.free(rebuilt_a);
    const rebuilt_b = try store.get(alloc, doc_b_index);
    defer alloc.free(rebuilt_b);

    try deleteColumnIndexesForRowRange(alloc, &store, "doc:b", "doc:c");
    const remaining_a = try store.get(alloc, doc_a_index);
    defer alloc.free(remaining_a);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_index));
}

test "relational column scan prune removes only entries whose base row is missing" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 1.0 },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 2.0 },
        },
    });
    defer alloc.free(row_b);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b);
    try store.putBatch(writes.items, deletes.items);

    const doc_a_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(doc_a_index);
    const doc_b_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(doc_b_index);
    const doc_b_row = try rowKeyAlloc(alloc, "doc:b");
    defer alloc.free(doc_b_row);

    try store.putBatch(&.{}, &.{doc_b_row});
    try pruneColumnIndexesForMissingRowsInRange(alloc, &store, "doc:a", "doc:c");

    const remaining_a = try store.get(alloc, doc_a_index);
    defer alloc.free(remaining_a);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_index));
}

test "relational column scan delete by document range uses reverse index entries" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 1.0 },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 2.0 },
        },
    });
    defer alloc.free(row_b);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a);
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b);
    try store.putBatch(writes.items, deletes.items);

    const doc_a_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(doc_a_index);
    const doc_b_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(doc_b_index);
    const doc_a_by_doc = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, "doc:a", "amount");
    defer alloc.free(doc_a_by_doc);
    const doc_b_by_doc = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, "doc:b", "amount");
    defer alloc.free(doc_b_by_doc);

    const before_b_reverse = try store.get(alloc, doc_b_by_doc);
    defer alloc.free(before_b_reverse);

    try deleteColumnIndexesByDocRange(alloc, &store, "doc:b", "doc:c");

    const remaining_a = try store.get(alloc, doc_a_index);
    defer alloc.free(remaining_a);
    const remaining_a_reverse = try store.get(alloc, doc_a_by_doc);
    defer alloc.free(remaining_a_reverse);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_index));
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_by_doc));
}
