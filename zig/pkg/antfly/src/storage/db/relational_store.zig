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
const doc_set = @import("doc_set.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const relational_collation = @import("relational_collation.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
const regex_mod = @import("antfly_regex");
const schema_mod = @import("../schema.zig");
const typed_dv = @import("../../section/typed_doc_values.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

const default_max_set_null_updates: usize = 4096;
const max_cascade_depth: usize = 64;
const max_cascade_deletes: usize = 4096;
const temporal_bound_neg_infinity_tag: u8 = 0xf0;
const temporal_bound_pos_infinity_tag: u8 = 0xf1;
pub const primary_key_constraint_name = "__antfly_primary_key__";

pub fn uniqueConstraintUsesDedicatedOwnerRows(constraint: schema_mod.UniqueConstraint) bool {
    return std.mem.eql(u8, constraint.name, primary_key_constraint_name) or constraint.without_overlaps_period != null;
}

pub fn uniqueConstraintUsesDedicatedOwnerRowsWithIndexes(
    constraint: schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
) bool {
    if (uniqueConstraintUsesDedicatedOwnerRows(constraint)) return true;
    return relationalIndexForUniqueConstraint(relational_indexes, constraint, null) == null;
}

/// Project a relational document into its serialized typed-row KV value and
/// track the buffer for cleanup. The row is always freshly allocated (the codec
/// owns no input bytes), so it is appended to `owned_values` unconditionally.
pub fn relationalStoreRowValueAlloc(
    alloc: Allocator,
    cleaned: []const u8,
    relational_columns: []const schema_mod.RelationalColumn,
    owned_values: *std.ArrayListUnmanaged([]u8),
) ![]const u8 {
    const row_value = try mapper.buildRelationalRowValueAlloc(alloc, cleaned, relational_columns);
    errdefer alloc.free(row_value);
    try owned_values.append(alloc, row_value);
    return row_value;
}

pub fn findUniqueConstraintMutation(
    mutations: []const types.UniqueConstraintMutation,
    constraint_name: []const u8,
    encoded_value: []const u8,
) ?types.UniqueConstraintMutation {
    for (mutations) |mutation| {
        if (std.mem.eql(u8, mutation.constraint_name, constraint_name) and std.mem.eql(u8, mutation.encoded_value, encoded_value)) return mutation;
    }
    return null;
}

pub const PredicateImplications = struct {
    predicates: []const schema_mod.RelationalCheck = &.{},
    expressions: []const types.RelationalRowsExpressionCondition = &.{},
};

pub const FilterCombineMode = enum {
    intersect,
    union_set,
    difference,
};

pub fn combineFilterSetFastAlloc(
    alloc: Allocator,
    current: *const doc_set.ResolvedDocSet,
    child: *const doc_set.ResolvedDocSet,
    mode: FilterCombineMode,
) !?doc_set.ResolvedDocSet {
    return switch (mode) {
        .intersect => try doc_set.intersectAlloc(alloc, current, child),
        .union_set => try doc_set.unionAlloc(alloc, current, child),
        .difference => try doc_set.differenceAlloc(alloc, current, child),
    };
}

pub fn predicatesImplyUniqueWhere(
    predicates: []const schema_mod.RelationalCheck,
    where_predicates: []const schema_mod.UniquePredicate,
) bool {
    for (where_predicates) |where_predicate| {
        if (!predicatesImplyUniquePredicate(predicates, where_predicate)) return false;
    }
    return true;
}

pub fn predicatesImplyUniqueWhereWithColumns(
    alloc: Allocator,
    predicates: []const schema_mod.RelationalCheck,
    where_predicates: []const schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    for (where_predicates) |where_predicate| {
        if (!(try predicatesImplyUniquePredicateWithColumns(alloc, predicates, where_predicate, columns))) return false;
    }
    return true;
}

fn predicatesImplyUniquePredicate(
    predicates: []const schema_mod.RelationalCheck,
    where_predicate: schema_mod.UniquePredicate,
) bool {
    for (predicates) |predicate| {
        if (!std.mem.eql(u8, predicate.field, where_predicate.field)) continue;
        switch (where_predicate.op) {
            .is_null => if (predicate.op == .is_null) return true,
            .is_not_null => if (predicate.op == .is_not_null or (predicate.op == .eq and predicate.value_json != null and jsonTextIsNonNull(predicate.value_json.?))) return true,
            .eq => if (predicate.op == .eq and optionalJsonTextEqual(predicate.value_json, where_predicate.value_json)) return true,
            .ne => if (predicate.op == .ne and optionalJsonTextEqual(predicate.value_json, where_predicate.value_json)) return true,
        }
    }
    return false;
}

fn predicatesImplyUniquePredicateWithColumns(
    alloc: Allocator,
    predicates: []const schema_mod.RelationalCheck,
    where_predicate: schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    const column = findRelationalColumn(columns, where_predicate.field);
    const collation = if (column) |resolved| resolved.collation else null;
    for (predicates) |predicate| {
        if (!std.mem.eql(u8, predicate.field, where_predicate.field)) continue;
        switch (where_predicate.op) {
            .is_null => if (predicate.op == .is_null) return true,
            .is_not_null => if (predicate.op == .is_not_null or (predicate.op == .eq and predicate.value_json != null and (try jsonTextIsNonNullAlloc(alloc, predicate.value_json.?)))) return true,
            .eq => if (predicate.op == .eq and (try optionalJsonTextEqualWithCollation(alloc, predicate.value_json, where_predicate.value_json, collation))) return true,
            .ne => if (predicate.op == .ne and (try optionalJsonTextEqualWithCollation(alloc, predicate.value_json, where_predicate.value_json, collation))) return true,
        }
    }
    return false;
}

fn optionalJsonTextEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn jsonTextIsNonNull(raw: []const u8) bool {
    return !std.mem.eql(u8, std.mem.trim(u8, raw, " \t\r\n"), "null");
}

fn jsonTextIsNonNullAlloc(alloc: Allocator, raw: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch return false;
    defer parsed.deinit();
    return parsed.value != .null;
}

fn optionalJsonTextEqualWithCollation(alloc: Allocator, a: ?[]const u8, b: ?[]const u8, collation: ?[]const u8) !bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    if (collation) |name| {
        if (relational_collation.isCaseInsensitive(name)) {
            var parsed_a = std.json.parseFromSlice(std.json.Value, alloc, a.?, .{}) catch return false;
            defer parsed_a.deinit();
            var parsed_b = std.json.parseFromSlice(std.json.Value, alloc, b.?, .{}) catch return false;
            defer parsed_b.deinit();
            if (parsed_a.value == .string and parsed_b.value == .string) return std.ascii.eqlIgnoreCase(parsed_a.value.string, parsed_b.value.string);
        }
    }
    return optionalJsonTextEqual(a, b);
}

pub const relational_identity_rewrite_intent_key_prefix = "\x00\x00__metadata__:txn_rel_identity_rewrite:";

pub fn relationalIdentityRewriteIntentKeyAlloc(alloc: Allocator, txn_id: types.TxnId, rewrite: types.RelationalIdentityRewrite) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, relational_identity_rewrite_intent_key_prefix);
    try appendHexBytes(alloc, &out, txn_id[0..]);
    try appendHexField(alloc, &out, rewrite.old_key);
    try appendHexField(alloc, &out, rewrite.new_key);
    return try out.toOwnedSlice(alloc);
}

pub fn isRelationalIdentityRewriteIntentKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, relational_identity_rewrite_intent_key_prefix);
}

pub fn encodeRelationalIdentityRewriteIntentValueAlloc(alloc: Allocator, rewrite: types.RelationalIdentityRewrite) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"old_key\":{f},\"new_key\":{f},\"value\":{f}}}",
        .{
            std.json.fmt(rewrite.old_key, .{}),
            std.json.fmt(rewrite.new_key, .{}),
            std.json.fmt(rewrite.value, .{}),
        },
    );
}

pub fn collectTransactionRelationalIdentityRewritesAlloc(
    alloc: Allocator,
    mutations: []const transactions_mod.OwnedIntentMutation,
    skip_keys: *std.ArrayListUnmanaged([]const u8),
) ![]types.RelationalIdentityRewrite {
    var out = std.ArrayListUnmanaged(types.RelationalIdentityRewrite).empty;
    errdefer {
        for (out.items) |*rewrite| freeRelationalIdentityRewrite(alloc, rewrite);
        out.deinit(alloc);
    }
    for (mutations) |mutation| {
        if (!isRelationalIdentityRewriteIntentKey(mutation.key)) continue;
        const raw = mutation.value orelse continue;
        const rewrite = try parseRelationalIdentityRewriteIntentValueAlloc(alloc, raw);
        var rewrite_owned = true;
        errdefer if (rewrite_owned) {
            var owned = rewrite;
            freeRelationalIdentityRewrite(alloc, &owned);
        };
        const skip_key = try alloc.dupe(u8, mutation.key);
        var skip_key_owned = true;
        errdefer if (skip_key_owned) alloc.free(skip_key);
        try skip_keys.append(alloc, skip_key);
        skip_key_owned = false;
        try out.append(alloc, rewrite);
        rewrite_owned = false;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn parseRelationalIdentityRewriteIntentValueAlloc(alloc: Allocator, raw: []const u8) !types.RelationalIdentityRewrite {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidQueryRequest,
    };
    const old_key = try alloc.dupe(u8, jsonObjectString(obj, "old_key") orelse return error.InvalidQueryRequest);
    errdefer alloc.free(old_key);
    const new_key = try alloc.dupe(u8, jsonObjectString(obj, "new_key") orelse return error.InvalidQueryRequest);
    errdefer alloc.free(new_key);
    const value = try alloc.dupe(u8, jsonObjectString(obj, "value") orelse return error.InvalidQueryRequest);
    errdefer alloc.free(value);
    return .{
        .old_key = old_key,
        .new_key = new_key,
        .value = value,
    };
}

pub fn freeRelationalIdentityRewrites(alloc: Allocator, rewrites: []types.RelationalIdentityRewrite) void {
    for (rewrites) |*rewrite| freeRelationalIdentityRewrite(alloc, rewrite);
    if (rewrites.len > 0) alloc.free(rewrites);
}

pub fn freeRelationalIdentityRewrite(alloc: Allocator, rewrite: *types.RelationalIdentityRewrite) void {
    alloc.free(@constCast(rewrite.old_key));
    alloc.free(@constCast(rewrite.new_key));
    alloc.free(@constCast(rewrite.value));
    rewrite.* = undefined;
}

pub fn isRelationalIdentityRewriteEndpoint(rewrites: []const types.RelationalIdentityRewrite, key: []const u8) bool {
    for (rewrites) |rewrite| {
        if (std.mem.eql(u8, rewrite.old_key, key) or std.mem.eql(u8, rewrite.new_key, key)) return true;
    }
    return false;
}

fn appendHexField(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.append(alloc, ':');
    try appendHexBytes(alloc, out, value);
}

fn appendHexBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const hex = "0123456789abcdef";
    try out.ensureUnusedCapacity(alloc, value.len * 2);
    for (value) |byte| {
        out.appendAssumeCapacity(hex[byte >> 4]);
        out.appendAssumeCapacity(hex[byte & 0x0f]);
    }
}

pub const row_claim_intent_key_prefix = "\x00\x00__metadata__:txn_row_claim:";

pub fn rowClaimIntentKeyAlloc(alloc: Allocator, row_key: []const u8) ![]u8 {
    return try std.mem.concat(alloc, u8, &.{ row_claim_intent_key_prefix, row_key });
}

pub fn rowClaimIntentValueAlloc(
    alloc: Allocator,
    txn_id: types.TxnId,
    claim: types.RowClaimRequest,
    now_ns: u64,
) ![]u8 {
    var txn_hex: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (txn_id, 0..) |byte, i| {
        txn_hex[i * 2] = hex[byte >> 4];
        txn_hex[i * 2 + 1] = hex[byte & 0x0f];
    }
    const lease_ns = std.math.mul(u64, claim.lease_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return try std.json.Stringify.valueAlloc(alloc, .{
        .version = @as(u32, 1),
        .mode = switch (claim.mode) {
            .for_update => "for_update",
            .for_no_key_update => "for_no_key_update",
            .for_share => "for_share",
            .for_key_share => "for_key_share",
        },
        .wait_policy = switch (claim.effectiveWaitPolicy()) {
            .wait => "wait",
            .nowait => "nowait",
            .skip_locked => "skip_locked",
        },
        .skip_locked = claim.effectiveSkipLocked(),
        .owner_id = claim.owner_id,
        .lease_ms = claim.lease_ms,
        .expires_at_ns = now_ns +| lease_ns,
        .txn_id = txn_hex[0..],
    }, .{});
}

pub fn rowClaimIntentPayloadExpired(alloc: Allocator, payload: []const u8, now_ns: u64) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, payload, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const version = parsed.value.object.get("version") orelse return false;
    if (version != .integer or version.integer != 1) return false;
    const expires_at = parsed.value.object.get("expires_at_ns") orelse return false;
    if (expires_at != .integer) return false;
    if (expires_at.integer < 0) return true;
    return @as(u64, @intCast(expires_at.integer)) <= now_ns;
}

pub fn appendRowClaimPredicatesForMutationKeys(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    writes: anytype,
    deletes: []const []const u8,
    comptime is_user_row_mutation_key: fn ([]const u8) bool,
) !void {
    for (writes) |write| {
        try appendRowClaimPredicateForKey(alloc, predicates, owned_keys, write.key, is_user_row_mutation_key);
    }
    for (deletes) |key| {
        try appendRowClaimPredicateForKey(alloc, predicates, owned_keys, key, is_user_row_mutation_key);
    }
}

pub fn appendRowClaimPredicatesForIdentityRewrites(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    rewrites: []const types.RelationalIdentityRewrite,
    comptime is_user_row_mutation_key: fn ([]const u8) bool,
) !void {
    for (rewrites) |rewrite| {
        try appendRowClaimPredicateForKey(alloc, predicates, owned_keys, rewrite.old_key, is_user_row_mutation_key);
        try appendRowClaimPredicateForKey(alloc, predicates, owned_keys, rewrite.new_key, is_user_row_mutation_key);
    }
}

fn appendRowClaimPredicateForKey(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    row_key: []const u8,
    comptime is_user_row_mutation_key: fn ([]const u8) bool,
) !void {
    if (!is_user_row_mutation_key(row_key)) return;
    const claim_key = try rowClaimIntentKeyAlloc(alloc, row_key);
    var claim_key_owned = true;
    errdefer if (claim_key_owned) alloc.free(claim_key);
    try owned_keys.append(alloc, claim_key);
    claim_key_owned = false;
    try predicates.append(alloc, .{
        .key = claim_key,
        .expected_version = 0,
    });
}

pub fn validateRelationalIdentityRewriteRequest(
    rewrites: []const types.RelationalIdentityRewrite,
    writes: []const types.TransactionWrite,
    deletes: []const []const u8,
    comptime is_user_row_mutation_key: fn ([]const u8) bool,
) !void {
    for (rewrites, 0..) |rewrite, i| {
        if (rewrite.old_key.len == 0 or rewrite.new_key.len == 0 or rewrite.value.len == 0) return error.InvalidQueryRequest;
        if (std.mem.eql(u8, rewrite.old_key, rewrite.new_key)) return error.UnsupportedOperation;
        if (!is_user_row_mutation_key(rewrite.old_key) or !is_user_row_mutation_key(rewrite.new_key)) return error.InvalidQueryRequest;
        if (containsTransactionWriteKey(writes, rewrite.old_key) or containsTransactionWriteKey(writes, rewrite.new_key)) return error.InvalidQueryRequest;
        if (containsKey(deletes, rewrite.old_key) or containsKey(deletes, rewrite.new_key)) return error.InvalidQueryRequest;
        for (rewrites[i + 1 ..]) |other| {
            if (std.mem.eql(u8, rewrite.old_key, other.old_key) or
                std.mem.eql(u8, rewrite.old_key, other.new_key) or
                std.mem.eql(u8, rewrite.new_key, other.old_key) or
                std.mem.eql(u8, rewrite.new_key, other.new_key))
            {
                return error.InvalidQueryRequest;
            }
        }
    }
}

pub const foreign_key_externalized_parent_check_intent_key_prefix = "\x00\x00__metadata__:txn_fk_externalized_parent_check:";
pub const foreign_key_constraint_timing_override_intent_key_prefix = "\x00\x00__metadata__:txn_fk_constraint_timing:";

pub fn foreignKeyExternalizedParentCheckIntentKeyAlloc(alloc: Allocator, txn_id: transactions_mod.TxnId, check: types.ForeignKeyParentCheck) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, foreign_key_externalized_parent_check_intent_key_prefix);
    try appendHexBytes(alloc, &out, txn_id[0..]);
    try appendHexField(alloc, &out, check.constraint_name);
    try appendHexField(alloc, &out, check.child_table);
    try appendHexField(alloc, &out, check.child_key);
    try appendHexField(alloc, &out, check.parent_table);
    try appendHexField(alloc, &out, check.parent_key);
    return try out.toOwnedSlice(alloc);
}

pub fn foreignKeyConstraintTimingOverrideIntentKeyAlloc(alloc: Allocator, txn_id: transactions_mod.TxnId, override: types.ForeignKeyConstraintTimingOverride) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, foreign_key_constraint_timing_override_intent_key_prefix);
    try appendHexBytes(alloc, &out, txn_id[0..]);
    try appendHexField(alloc, &out, override.constraint_name);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeForeignKeyExternalizedParentCheckIntentValueAlloc(alloc: Allocator, check: types.ForeignKeyParentCheck) ![]u8 {
    const timing = switch (check.timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"child_table\":{f},\"child_key\":{f},\"parent_table\":{f},\"parent_key\":{f},\"timing\":{f}",
        .{
            std.json.fmt(check.constraint_name, .{}),
            std.json.fmt(check.child_table, .{}),
            std.json.fmt(check.child_key, .{}),
            std.json.fmt(check.parent_table, .{}),
            std.json.fmt(check.parent_key, .{}),
            std.json.fmt(timing, .{}),
        },
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    if (check.parent_constraint_name) |name| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"parent_constraint_name\":{f}", .{std.json.fmt(name, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (check.ordered_parent_tuple) |tuple| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"ordered_parent_tuple\":{f}", .{std.json.fmt(tuple, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (check.child_period_start_json) |json| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"child_period_start\":{s}", .{json});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (check.child_period_end_json) |json| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"child_period_end\":{s}", .{json});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc: Allocator, override: types.ForeignKeyConstraintTimingOverride) ![]u8 {
    const timing = switch (override.timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
    return try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"timing\":{f}}}",
        .{
            std.json.fmt(override.constraint_name, .{}),
            std.json.fmt(timing, .{}),
        },
    );
}

pub fn collectTransactionExternalizedForeignKeyParentChecksAlloc(
    alloc: Allocator,
    mutations: []const transactions_mod.OwnedIntentMutation,
    skip_keys: *std.ArrayListUnmanaged([]const u8),
) ![]ExternalizedForeignKeyParentCheck {
    var out = std.ArrayListUnmanaged(ExternalizedForeignKeyParentCheck).empty;
    errdefer {
        for (out.items) |*check| freeExternalizedForeignKeyParentCheck(alloc, check);
        out.deinit(alloc);
    }
    for (mutations) |mutation| {
        if (!isForeignKeyExternalizedParentCheckIntentKey(mutation.key)) continue;
        const raw = mutation.value orelse continue;
        const check = try parseForeignKeyExternalizedParentCheckIntentValueAlloc(alloc, raw);
        var check_owned = true;
        errdefer if (check_owned) {
            var owned = check;
            freeExternalizedForeignKeyParentCheck(alloc, &owned);
        };
        const skip_key = try alloc.dupe(u8, mutation.key);
        var skip_key_owned = true;
        errdefer if (skip_key_owned) alloc.free(skip_key);
        try skip_keys.append(alloc, skip_key);
        skip_key_owned = false;
        try out.append(alloc, check);
        check_owned = false;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn collectTransactionForeignKeyConstraintTimingOverridesAlloc(
    alloc: Allocator,
    mutations: []const transactions_mod.OwnedIntentMutation,
    skip_keys: *std.ArrayListUnmanaged([]const u8),
) ![]ForeignKeyConstraintTimingOverride {
    var out = std.ArrayListUnmanaged(ForeignKeyConstraintTimingOverride).empty;
    errdefer {
        for (out.items) |*override| freeRelationalForeignKeyConstraintTimingOverride(alloc, override);
        out.deinit(alloc);
    }
    for (mutations) |mutation| {
        if (!isForeignKeyConstraintTimingOverrideIntentKey(mutation.key)) continue;
        const raw = mutation.value orelse continue;
        const override = try parseForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc, raw);
        var override_owned = true;
        errdefer if (override_owned) {
            var owned = override;
            freeRelationalForeignKeyConstraintTimingOverride(alloc, &owned);
        };
        const skip_key = try alloc.dupe(u8, mutation.key);
        var skip_key_owned = true;
        errdefer if (skip_key_owned) alloc.free(skip_key);
        try skip_keys.append(alloc, skip_key);
        skip_key_owned = false;
        try out.append(alloc, override);
        override_owned = false;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn freeExternalizedForeignKeyParentChecks(alloc: Allocator, checks: []ExternalizedForeignKeyParentCheck) void {
    for (checks) |*check| freeExternalizedForeignKeyParentCheck(alloc, check);
    if (checks.len > 0) alloc.free(checks);
}

pub fn freeExternalizedForeignKeyParentCheck(alloc: Allocator, check: *ExternalizedForeignKeyParentCheck) void {
    alloc.free(@constCast(check.constraint_name));
    alloc.free(@constCast(check.child_table));
    alloc.free(@constCast(check.child_key));
    alloc.free(@constCast(check.parent_table));
    alloc.free(@constCast(check.parent_key));
    if (check.parent_constraint_name) |name| alloc.free(@constCast(name));
    if (check.ordered_parent_tuple) |tuple| alloc.free(@constCast(tuple));
    if (check.child_period_start_json) |json| alloc.free(@constCast(json));
    if (check.child_period_end_json) |json| alloc.free(@constCast(json));
    check.* = undefined;
}

pub fn freeRelationalForeignKeyConstraintTimingOverrides(alloc: Allocator, overrides: []ForeignKeyConstraintTimingOverride) void {
    for (overrides) |*override| freeRelationalForeignKeyConstraintTimingOverride(alloc, override);
    if (overrides.len > 0) alloc.free(overrides);
}

pub fn freeRelationalForeignKeyConstraintTimingOverride(alloc: Allocator, override: *ForeignKeyConstraintTimingOverride) void {
    alloc.free(@constCast(override.constraint_name));
    override.* = undefined;
}

fn parseForeignKeyExternalizedParentCheckIntentValueAlloc(alloc: Allocator, raw: []const u8) !ExternalizedForeignKeyParentCheck {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.ForeignKeyViolation,
    };
    const timing = jsonObjectString(obj, "timing") orelse return error.ForeignKeyViolation;
    if (!std.mem.eql(u8, timing, "deferred") and !std.mem.eql(u8, timing, "immediate")) return error.ForeignKeyViolation;
    const constraint_name = try alloc.dupe(u8, jsonObjectString(obj, "constraint_name") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(constraint_name);
    const child_table = try alloc.dupe(u8, jsonObjectString(obj, "child_table") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(child_table);
    const child_key = try alloc.dupe(u8, jsonObjectString(obj, "child_key") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(child_key);
    const parent_table = try alloc.dupe(u8, jsonObjectString(obj, "parent_table") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(parent_table);
    const parent_key = try alloc.dupe(u8, jsonObjectString(obj, "parent_key") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(parent_key);
    const parent_constraint_name = if (jsonObjectString(obj, "parent_constraint_name")) |name|
        try alloc.dupe(u8, name)
    else
        null;
    errdefer if (parent_constraint_name) |name| alloc.free(name);
    const ordered_parent_tuple = if (jsonObjectString(obj, "ordered_parent_tuple")) |tuple|
        try alloc.dupe(u8, tuple)
    else
        null;
    errdefer if (ordered_parent_tuple) |tuple| alloc.free(tuple);
    const child_period_start_json = if (obj.get("child_period_start")) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    errdefer if (child_period_start_json) |json| alloc.free(json);
    const child_period_end_json = if (obj.get("child_period_end")) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    errdefer if (child_period_end_json) |json| alloc.free(json);
    if ((child_period_start_json == null) != (child_period_end_json == null)) return error.ForeignKeyViolation;
    return .{
        .constraint_name = constraint_name,
        .child_table = child_table,
        .child_key = child_key,
        .parent_table = parent_table,
        .parent_key = parent_key,
        .parent_constraint_name = parent_constraint_name,
        .ordered_parent_tuple = ordered_parent_tuple,
        .child_period_start_json = child_period_start_json,
        .child_period_end_json = child_period_end_json,
        .timing = if (std.mem.eql(u8, timing, "deferred")) .deferred else .immediate,
    };
}

fn parseForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc: Allocator, raw: []const u8) !ForeignKeyConstraintTimingOverride {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.ForeignKeyViolation,
    };
    const timing = jsonObjectString(obj, "timing") orelse return error.ForeignKeyViolation;
    if (!std.mem.eql(u8, timing, "deferred") and !std.mem.eql(u8, timing, "immediate")) return error.ForeignKeyViolation;
    const constraint_name = try alloc.dupe(u8, jsonObjectString(obj, "constraint_name") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(constraint_name);
    return .{
        .constraint_name = constraint_name,
        .timing = if (std.mem.eql(u8, timing, "deferred")) .deferred else .immediate,
    };
}

fn isForeignKeyExternalizedParentCheckIntentKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, foreign_key_externalized_parent_check_intent_key_prefix);
}

fn isForeignKeyConstraintTimingOverrideIntentKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, foreign_key_constraint_timing_override_intent_key_prefix);
}

fn containsTransactionWriteKey(writes: []const types.TransactionWrite, key: []const u8) bool {
    for (writes) |write| {
        if (std.mem.eql(u8, write.key, key)) return true;
    }
    return false;
}

fn jsonObjectString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

pub const TemporalBound = union(enum) {
    neg_infinity,
    number: f64,
    datetime_ns: i64,
    pos_infinity,
};

pub const TemporalSpan = struct {
    start: TemporalBound,
    end: TemporalBound,
};

pub fn temporalStartBoundFromJsonAlloc(
    alloc: Allocator,
    value_json: []const u8,
    column: schema_mod.RelationalColumn,
) !TemporalBound {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try temporalStartBoundFromJsonValue(parsed.value, column);
}

pub fn temporalEndBoundFromJsonAlloc(
    alloc: Allocator,
    value_json: []const u8,
    column: schema_mod.RelationalColumn,
) !TemporalBound {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try temporalEndBoundFromJsonValue(parsed.value, column);
}

pub fn temporalStartBoundFromJsonValue(value: ?std.json.Value, column: schema_mod.RelationalColumn) !TemporalBound {
    if (value) |present| {
        if (present != .null) return try temporalFiniteBoundFromJsonValue(present, column);
    }
    return if (column.nullable) .neg_infinity else error.InvalidQueryRequest;
}

pub fn temporalEndBoundFromJsonValue(value: ?std.json.Value, column: schema_mod.RelationalColumn) !TemporalBound {
    if (value) |present| {
        if (present != .null) return try temporalFiniteBoundFromJsonValue(present, column);
    }
    return if (column.nullable) .pos_infinity else error.InvalidQueryRequest;
}

fn temporalFiniteBoundFromJsonValue(value: std.json.Value, column: schema_mod.RelationalColumn) !TemporalBound {
    if (value == .null) return error.InvalidQueryRequest;
    return switch (column.field_type) {
        .numeric => switch (value) {
            .integer => |number| .{ .number = @floatFromInt(number) },
            .float => |number| .{ .number = number },
            else => error.InvalidQueryRequest,
        },
        .datetime => switch (value) {
            .integer => |number| .{ .datetime_ns = number },
            else => error.InvalidQueryRequest,
        },
        else => error.InvalidQueryRequest,
    };
}

pub fn temporalSpanValid(span: TemporalSpan) bool {
    return temporalBoundLessThan(span.start, span.end);
}

pub fn temporalSpansOverlap(left: TemporalSpan, right: TemporalSpan) bool {
    return temporalBoundLessThan(left.start, right.end) and temporalBoundLessThan(right.start, left.end);
}

pub fn temporalBoundLessThan(left: TemporalBound, right: TemporalBound) bool {
    return switch (left) {
        .neg_infinity => right != .neg_infinity,
        .number => |value| switch (right) {
            .neg_infinity => false,
            .number => |other| value < other,
            .pos_infinity => true,
            else => false,
        },
        .datetime_ns => |value| switch (right) {
            .neg_infinity => false,
            .datetime_ns => |other| value < other,
            .pos_infinity => true,
            else => false,
        },
        .pos_infinity => false,
    };
}

pub fn temporalBoundEqual(left: TemporalBound, right: TemporalBound) bool {
    return switch (left) {
        .neg_infinity => right == .neg_infinity,
        .number => |value| switch (right) {
            .number => |other| value == other,
            else => false,
        },
        .datetime_ns => |value| switch (right) {
            .datetime_ns => |other| value == other,
            else => false,
        },
        .pos_infinity => right == .pos_infinity,
    };
}

pub fn temporalBoundMax(left: TemporalBound, right: TemporalBound) TemporalBound {
    return if (temporalBoundLessThan(left, right)) right else left;
}

pub fn temporalBoundMin(left: TemporalBound, right: TemporalBound) TemporalBound {
    return if (temporalBoundLessThan(left, right)) left else right;
}

pub fn findPeriod(periods: []const schema_mod.RelationalPeriod, name: []const u8) ?schema_mod.RelationalPeriod {
    for (periods) |period| {
        if (std.mem.eql(u8, period.name, name)) return period;
    }
    return null;
}

pub fn findTemporalColumn(columns: []const schema_mod.RelationalColumn, path: []const u8) ?schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, path) or std.mem.eql(u8, column.name, path)) return column;
    }
    return null;
}

pub fn columnForField(runtime_schema: schema_mod.TableSchema, field: []const u8) ?schema_mod.RelationalColumn {
    const normalized = if (std.mem.startsWith(u8, field, "/")) field[1..] else field;
    for (runtime_schema.relational_columns) |column| {
        if (std.mem.eql(u8, field, column.name) or
            std.mem.eql(u8, field, column.path) or
            std.mem.eql(u8, normalized, column.name) or
            std.mem.eql(u8, normalized, column.path))
        {
            return column;
        }
    }
    return null;
}

pub fn jsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

pub fn jsonNumberAsU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @intCast(integer) else null,
        else => null,
    };
}

pub fn jsonNumberAsI64(value: std.json.Value) ?i64 {
    return switch (value) {
        .integer => |integer| integer,
        .number_string => |text| std.fmt.parseInt(i64, text, 10) catch null,
        else => null,
    };
}

pub fn arrayColumnValueContains(
    alloc: Allocator,
    value: OwnedColumnValue,
    wanted: std.json.Value,
) !bool {
    if (value.value_type != .bytes_val or !value.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    for (parsed.value.array.items) |item| {
        if (jsonValuesEqual(item, wanted)) return true;
    }
    return false;
}

pub fn arrayColumnValueContainsAll(
    alloc: Allocator,
    value: OwnedColumnValue,
    wanted: std.json.Value,
) !bool {
    if (wanted != .array) return error.InvalidQueryRequest;
    if (value.value_type != .bytes_val or !value.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    return jsonValueContains(parsed.value, wanted);
}

pub fn arrayColumnValueEquals(
    alloc: Allocator,
    value: OwnedColumnValue,
    wanted: std.json.Value,
) !bool {
    if (wanted != .array) return error.InvalidQueryRequest;
    if (value.value_type != .bytes_val or !value.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    return jsonValuesEqualExact(parsed.value, wanted);
}

fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    return switch (lhs) {
        .null => rhs == .null,
        .bool => |value| rhs == .bool and rhs.bool == value,
        .integer => |value| switch (rhs) {
            .integer => |other| other == value,
            .float => |other| @as(f64, @floatFromInt(value)) == other,
            else => false,
        },
        .float => |value| switch (rhs) {
            .integer => |other| value == @as(f64, @floatFromInt(other)),
            .float => |other| other == value,
            else => false,
        },
        .number_string => |value| switch (rhs) {
            .number_string => |other| std.mem.eql(u8, value, other),
            .string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .string => |value| switch (rhs) {
            .string => |other| std.mem.eql(u8, value, other),
            .number_string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .array => |array| blk: {
            if (rhs != .array or array.items.len != rhs.array.items.len) break :blk false;
            for (array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqual(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (rhs != .object or object.count() != rhs.object.count()) break :blk false;
            for (object.keys(), object.values()) |key, lhs_value| {
                const rhs_value = rhs.object.get(key) orelse break :blk false;
                if (!jsonValuesEqual(lhs_value, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

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

pub const RowRewriteRename = struct {
    old_path: []const u8,
    new_path: []const u8,
};

pub const RowRewriteSet = struct {
    cell: relational_row_codec.Cell,
    only_if_missing: bool = false,
};

pub const RowRewritePlan = struct {
    renames: []const RowRewriteRename = &.{},
    drops: []const []const u8 = &.{},
    sets: []const RowRewriteSet = &.{},
};

pub const RowRewriteReport = struct {
    scanned_rows: u64 = 0,
    rewritten_rows: u64 = 0,
    unchanged_rows: u64 = 0,
    renamed_cells: u64 = 0,
    dropped_cells: u64 = 0,
    set_cells: u64 = 0,
};

pub const ForeignKeyIntegrityMode = enum {
    validate,
    dry_run,
    repair,
};

pub const ExternalizedForeignKeyParentCheck = struct {
    constraint_name: []const u8,
    child_table: []const u8,
    child_key: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    parent_constraint_name: ?[]const u8 = null,
    ordered_parent_tuple: ?[]const u8 = null,
    child_period_start_json: ?[]const u8 = null,
    child_period_end_json: ?[]const u8 = null,
    timing: schema_mod.ForeignKeyTiming = .immediate,
};

pub const ForeignKeyConstraintTimingOverride = struct {
    constraint_name: []const u8,
    timing: schema_mod.ForeignKeyTiming,
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

pub const UniqueConstraintIntegrityReport = struct {
    scanned_rows: u64 = 0,
    expected_unique_rows: u64 = 0,
    scanned_unique_rows: u64 = 0,
    missing_unique_rows: u64 = 0,
    stale_unique_rows: u64 = 0,
    duplicate_unique_rows: u64 = 0,
    repaired_unique_rows: u64 = 0,
    deleted_stale_unique_rows: u64 = 0,

    pub fn valid(self: UniqueConstraintIntegrityReport) bool {
        return self.missing_unique_rows == 0 and
            self.stale_unique_rows == 0 and
            self.duplicate_unique_rows == 0;
    }
};

pub const SecondaryIndexRebuildReport = struct {
    scanned_rows: u64 = 0,
    indexed_rows: u64 = 0,
    deleted_entries: u64 = 0,
    written_entries: u64 = 0,
};

pub const ColumnBackedIndexRepairReport = struct {
    scanned_rows: u64 = 0,
    indexed_rows: u64 = 0,
    deleted_orphan_entries: u64 = 0,
    written_entries: u64 = 0,
};

pub const SecondaryIndexRebuildPage = struct {
    report: SecondaryIndexRebuildReport = .{},
    complete: bool = true,
    next_start_doc_key: []u8 = "",

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.next_start_doc_key.len > 0) alloc.free(self.next_start_doc_key);
        self.* = undefined;
    }
};

const PeriodBound = union(enum) {
    neg_infinity,
    f64_val: f64,
    i64_val: i64,
    pos_infinity,
};

const PeriodSpan = struct {
    start: PeriodBound,
    end: PeriodBound,
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
    indexes: []const schema_mod.RelationalIndex = &.{},
    target_index_name: ?[]const u8 = null,
    target_index_generation: u64 = 0,

    pub fn empty() ColumnIndexPolicy {
        return .{};
    }

    fn maintainsLegacyScalarIndexes(self: ColumnIndexPolicy) bool {
        return self.columns.len == 0 and self.indexes.len == 0 and self.target_index_name == null;
    }

    pub fn fromSchemaParts(columns: []const schema_mod.RelationalColumn, indexes: []const schema_mod.RelationalIndex) ColumnIndexPolicy {
        return .{
            .columns = columns,
            .indexes = indexes,
        };
    }

    pub fn fromSchema(schema: schema_mod.TableSchema) ColumnIndexPolicy {
        return .{
            .columns = schema.relational_columns,
            .indexes = schema.relational_indexes,
        };
    }

    pub fn forIndexRebuild(columns: []const schema_mod.RelationalColumn, indexes: []const schema_mod.RelationalIndex, index_name: []const u8, index_generation: u64) ColumnIndexPolicy {
        return .{
            .columns = columns,
            .indexes = indexes,
            .target_index_name = index_name,
            .target_index_generation = index_generation,
        };
    }

    pub fn shouldMaintainIndexColumn(self: ColumnIndexPolicy, column: schema_mod.RelationalColumn) bool {
        if (self.target_index_name) |target| {
            if (self.catalogIndexByName(target)) |index| {
                if (index.owner_kind != .relational_column) return false;
                if (!std.mem.eql(u8, index.owner_name, column.name) and !std.mem.eql(u8, index.owner_name, column.path)) return false;
                if (self.target_index_generation != 0 and index.generation != self.target_index_generation) return false;
                return true;
            }
            return false;
        }
        return true;
    }

    pub fn shouldMaintainCatalogIndex(self: ColumnIndexPolicy, index: schema_mod.RelationalIndex) bool {
        if (self.target_index_name) |target| {
            if (!relationalIndexNameMatches(index, target)) return false;
            if (self.target_index_generation != 0 and index.generation != self.target_index_generation) return false;
        }
        return true;
    }

    pub fn mayIndexAnyRow(self: ColumnIndexPolicy) bool {
        if (self.maintainsLegacyScalarIndexes()) return true;
        for (self.columns) |column| {
            if (!self.shouldMaintainIndexColumn(column)) continue;
            if (self.catalogIndexForMaintainedColumn(column)) |_| return true;
        }
        return false;
    }

    pub fn hasConditionalIndexes(self: ColumnIndexPolicy) bool {
        for (self.columns) |column| {
            if (self.catalogIndexForMaintainedColumn(column)) |index| {
                if (index.where.len != 0 or index.where_expressions.len != 0) return true;
            }
        }
        return false;
    }

    pub fn canSkipUnchangedRowDerivedMaintenance(self: ColumnIndexPolicy) bool {
        for (self.columns) |column| {
            if (self.catalogIndexForMaintainedColumn(column)) |index| {
                switch (index.access_method) {
                    .scalar_column, .ordered_tuple => if (!relationalIndexReady(index)) return false,
                    .algebraic_filter, .text_search => return false,
                }
            }
        }
        return true;
    }

    pub fn shouldIndex(self: ColumnIndexPolicy, path: []const u8) bool {
        if (self.maintainsLegacyScalarIndexes()) return true;
        for (self.columns) |column| {
            if (std.mem.eql(u8, column.path, path) or std.mem.eql(u8, column.name, path)) {
                return self.catalogIndexForColumn(column, null) != null;
            }
        }
        return false;
    }

    pub fn shouldMaintainScalarIndexRow(self: ColumnIndexPolicy, alloc: Allocator, path: []const u8, row_value: []const u8) !bool {
        if (self.maintainsLegacyScalarIndexes()) return true;
        for (self.columns) |column| {
            if (!std.mem.eql(u8, column.path, path) and !std.mem.eql(u8, column.name, path)) continue;
            if (!self.shouldMaintainIndexColumn(column)) return false;
            if (!self.columnHasScalarIndex(column)) return false;
            return try self.shouldIndexRow(alloc, path, row_value);
        }
        return false;
    }

    pub fn shouldIndexRow(self: ColumnIndexPolicy, alloc: Allocator, path: []const u8, row_value: []const u8) !bool {
        if (self.maintainsLegacyScalarIndexes()) return true;
        for (self.columns) |column| {
            if (!std.mem.eql(u8, column.path, path) and !std.mem.eql(u8, column.name, path)) continue;
            if (!self.shouldMaintainIndexColumn(column)) return false;
            const index = self.catalogIndexForColumn(column, null);
            if (index == null) return false;
            const predicates = index.?.where;
            const expression_predicates = index.?.where_expressions;
            if (predicates.len == 0 and expression_predicates.len == 0) return true;
            if (predicates.len != 0 and !(try rowMatchesUniqueConstraintPredicates(alloc, row_value, predicates, self.columns))) return false;
            if (expression_predicates.len != 0 and !(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, expression_predicates, self.columns))) return false;
            return true;
        }
        return false;
    }

    pub fn shouldIndexCatalogRow(self: ColumnIndexPolicy, alloc: Allocator, index: schema_mod.RelationalIndex, row_value: []const u8) !bool {
        if (!self.shouldMaintainCatalogIndex(index)) return false;
        if (index.where.len == 0 and index.where_expressions.len == 0) return true;
        if (index.where.len != 0 and !(try rowMatchesUniqueConstraintPredicates(alloc, row_value, index.where, self.columns))) return false;
        if (index.where_expressions.len != 0 and !(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, index.where_expressions, self.columns))) return false;
        return true;
    }

    pub fn readyForQuery(self: ColumnIndexPolicy, path: []const u8) bool {
        if (self.maintainsLegacyScalarIndexes()) return true;
        for (self.columns) |column| {
            if (!std.mem.eql(u8, column.path, path) and !std.mem.eql(u8, column.name, path)) continue;
            if (self.catalogIndexForColumn(column, .scalar_column)) |index| {
                return relationalIndexReady(index);
            }
            return false;
        }
        return false;
    }

    pub fn indexForRebuild(self: ColumnIndexPolicy, index_name: []const u8, index_generation: u64) !schema_mod.RelationalIndex {
        if (self.catalogIndexByName(index_name)) |index| {
            if (index.owner_kind != .relational_column) return error.RelationalIndexNotFound;
            if (!relationalIndexRebuildable(index)) return error.RelationalIndexNotBuilding;
            if (index.generation != index_generation) return error.RelationalIndexGenerationMismatch;
            _ = self.columnByName(index.owner_name) orelse return error.RelationalIndexNotFound;
            return index;
        }
        return error.RelationalIndexNotFound;
    }

    fn columnHasScalarIndex(self: ColumnIndexPolicy, column: schema_mod.RelationalColumn) bool {
        if (self.catalogIndexForColumn(column, .scalar_column)) |_| return true;
        return false;
    }

    fn uniqueConstraintIndex(self: ColumnIndexPolicy, constraint: schema_mod.UniqueConstraint, access_method: ?schema_mod.RelationalIndexAccessMethod) ?schema_mod.RelationalIndex {
        return relationalIndexForUniqueConstraint(self.indexes, constraint, access_method);
    }

    fn catalogIndexForMaintainedColumn(self: ColumnIndexPolicy, column: schema_mod.RelationalColumn) ?schema_mod.RelationalIndex {
        const index = self.catalogIndexForColumn(column, null) orelse return null;
        return switch (index.access_method) {
            .scalar_column, .ordered_tuple => if (relationalIndexMaintained(index)) index else null,
            .algebraic_filter, .text_search => null,
        };
    }

    fn catalogIndexForColumn(self: ColumnIndexPolicy, column: schema_mod.RelationalColumn, access_method: ?schema_mod.RelationalIndexAccessMethod) ?schema_mod.RelationalIndex {
        for (self.indexes) |index| {
            if (index.owner_kind != .relational_column) continue;
            if (access_method != null and index.access_method != access_method.?) continue;
            if (!std.mem.eql(u8, index.owner_name, column.name) and !std.mem.eql(u8, index.owner_name, column.path)) continue;
            return index;
        }
        return null;
    }

    fn catalogIndexByName(self: ColumnIndexPolicy, index_name: []const u8) ?schema_mod.RelationalIndex {
        for (self.indexes) |index| {
            if (relationalIndexNameMatches(index, index_name)) return index;
        }
        return null;
    }

    fn columnByName(self: ColumnIndexPolicy, name: []const u8) ?schema_mod.RelationalColumn {
        for (self.columns) |column| {
            if (std.mem.eql(u8, column.name, name) or std.mem.eql(u8, column.path, name)) return column;
        }
        return null;
    }

    fn orderedTupleIndexOwnerColumn(self: ColumnIndexPolicy, index: schema_mod.RelationalIndex) ?schema_mod.RelationalColumn {
        if (index.owner_kind != .relational_column) return null;
        return self.columnByName(index.owner_name);
    }
};

fn relationalIndexNameMatches(index: schema_mod.RelationalIndex, index_name: []const u8) bool {
    return std.mem.eql(u8, index.name, index_name) or std.mem.eql(u8, index.owner_name, index_name);
}

pub fn relationalIndexForUniqueConstraint(
    indexes: []const schema_mod.RelationalIndex,
    constraint: schema_mod.UniqueConstraint,
    access_method: ?schema_mod.RelationalIndexAccessMethod,
) ?schema_mod.RelationalIndex {
    for (indexes) |index| {
        if (index.owner_kind != .unique_constraint) continue;
        if (access_method != null and index.access_method != access_method.?) continue;
        if (!std.mem.eql(u8, index.owner_name, constraint.name) and !std.mem.eql(u8, index.name, constraint.name)) continue;
        return index;
    }
    return null;
}

pub fn primaryKeyAsUniqueConstraint(primary_key: schema_mod.PrimaryKey) schema_mod.UniqueConstraint {
    return .{
        .name = primary_key_constraint_name,
        .columns = primary_key.columns,
        .without_overlaps_period = primary_key.without_overlaps_period,
    };
}

pub fn primaryKeyTupleValueAlloc(alloc: Allocator, row_value: []const u8, primary_key: schema_mod.PrimaryKey) ![]u8 {
    return try requiredConstraintColumnsTupleValueAlloc(alloc, row_value, primary_key.columns);
}

fn primaryKeyTupleValueFromCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    primary_key: schema_mod.PrimaryKey,
) ![]u8 {
    return try requiredConstraintColumnsTupleValueFromCellsAlloc(alloc, cells, primary_key.columns);
}

pub fn bytesTupleValueAlloc(alloc: Allocator, values: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (values) |value| {
        var component = std.ArrayListUnmanaged(u8).empty;
        defer component.deinit(alloc);
        try component.append(alloc, @intFromEnum(typed_dv.ValueType.bytes_val));
        try component.appendSlice(alloc, value);
        try internal_keys.appendEncodedComponent(&out, alloc, component.items);
    }
    return try out.toOwnedSlice(alloc);
}

pub const OrderedTupleIndexEntryKeys = struct {
    encoded_tuple: []u8,
    forward_key: []u8,
    reverse_key: []u8,
    forward_value: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.encoded_tuple.len != 0) alloc.free(self.encoded_tuple);
        if (self.forward_key.len != 0) alloc.free(self.forward_key);
        if (self.reverse_key.len != 0) alloc.free(self.reverse_key);
        if (self.forward_value.len != 0) alloc.free(self.forward_value);
        self.* = undefined;
    }
};

pub fn orderedTupleIndexEntryKeysForRowAlloc(
    alloc: Allocator,
    index_id: []const u8,
    doc_key: []const u8,
    row_value: []const u8,
    index_keys: []const schema_mod.RelationalIndexKey,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleIndexEntryKeys {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    return try orderedTupleIndexEntryKeysForCellsAlloc(alloc, index_id, doc_key, row.cells, index_keys, &.{}, columns);
}

fn orderedTupleIndexEntryKeysForCellsAlloc(
    alloc: Allocator,
    index_id: []const u8,
    doc_key: []const u8,
    cells: []const relational_row_codec.Cell,
    index_keys: []const schema_mod.RelationalIndexKey,
    include_columns: []const []const u8,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleIndexEntryKeys {
    return try orderedTupleIndexEntryKeysForCellsWithMetadataAlloc(alloc, index_id, doc_key, cells, index_keys, include_columns, columns, 0, 0, null);
}

fn orderedTupleIndexEntryKeysForCellsWithMetadataAlloc(
    alloc: Allocator,
    index_id: []const u8,
    doc_key: []const u8,
    cells: []const relational_row_codec.Cell,
    index_keys: []const schema_mod.RelationalIndexKey,
    include_columns: []const []const u8,
    columns: []const schema_mod.RelationalColumn,
    row_generation: u64,
    index_generation: u64,
    schema_fingerprint: ?[]const u8,
) !OrderedTupleIndexEntryKeys {
    const encoded_tuple = try orderedTupleValueForIndexKeyCellsAlloc(alloc, cells, index_keys, columns);
    errdefer alloc.free(encoded_tuple);
    const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, index_id, encoded_tuple, doc_key);
    errdefer alloc.free(forward_key);
    const reverse_key = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, doc_key, index_id, encoded_tuple);
    errdefer alloc.free(reverse_key);
    const forward_value = try orderedTupleIncludePayloadForCellsAlloc(alloc, cells, include_columns, columns, row_generation, index_generation, schema_fingerprint);
    return .{
        .encoded_tuple = encoded_tuple,
        .forward_key = forward_key,
        .reverse_key = reverse_key,
        .forward_value = forward_value,
    };
}

fn orderedTupleIndexEntryKeysForEncodedTupleAlloc(
    alloc: Allocator,
    index_id: []const u8,
    doc_key: []const u8,
    encoded_tuple: []u8,
    cells: []const relational_row_codec.Cell,
    include_columns: []const []const u8,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleIndexEntryKeys {
    errdefer alloc.free(encoded_tuple);
    const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, index_id, encoded_tuple, doc_key);
    errdefer alloc.free(forward_key);
    const reverse_key = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, doc_key, index_id, encoded_tuple);
    errdefer alloc.free(reverse_key);
    const forward_value = try orderedTupleIncludePayloadForCellsAlloc(alloc, cells, include_columns, columns, 0, 0, null);
    return .{
        .encoded_tuple = encoded_tuple,
        .forward_key = forward_key,
        .reverse_key = reverse_key,
        .forward_value = forward_value,
    };
}

fn orderedTupleUniqueIndexEntryKeysForCellsAlloc(
    alloc: Allocator,
    constraint: schema_mod.UniqueConstraint,
    index: schema_mod.RelationalIndex,
    doc_key: []const u8,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    columns: []const schema_mod.RelationalColumn,
) !?OrderedTupleIndexEntryKeys {
    if (!(try rowMatchesUniqueConstraintPredicatesFromCells(alloc, cells, index.where, columns))) return null;
    if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, index.where_expressions, columns))) return null;
    if (constraint.expressions.len == 0) {
        return try orderedTupleIndexEntryKeysForCellsAlloc(alloc, index.name, doc_key, cells, index.keys, index.include_columns, columns);
    }
    const encoded_tuple = (try uniqueConstraintExpressionTupleValueFromCellsAlloc(alloc, row_value, cells, constraint, columns)) orelse return null;
    return try orderedTupleIndexEntryKeysForEncodedTupleAlloc(alloc, index.name, doc_key, encoded_tuple, cells, index.include_columns, columns);
}

fn orderedTupleUniqueIndexTupleValueForCellsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    constraint: schema_mod.UniqueConstraint,
    index: schema_mod.RelationalIndex,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (!(try rowMatchesUniqueConstraintPredicatesFromCells(alloc, cells, index.where, columns))) return null;
    if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, index.where_expressions, columns))) return null;
    if (constraint.expressions.len == 0) return try orderedTupleValueForIndexKeyCellsAlloc(alloc, cells, index.keys, columns);
    return try uniqueConstraintExpressionTupleValueFromCellsAlloc(alloc, row_value, cells, constraint, columns);
}

pub fn orderedTupleUniqueIndexTupleValueAlloc(
    alloc: Allocator,
    row_value: []const u8,
    constraint: schema_mod.UniqueConstraint,
    index: schema_mod.RelationalIndex,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    return try orderedTupleUniqueIndexTupleValueForCellsAlloc(alloc, row_value, row.cells, constraint, index, columns);
}

fn uniqueConstraintExpressionTupleValueFromCellsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    constraint: schema_mod.UniqueConstraint,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    const key_columns = if (std.mem.eql(u8, constraint.name, primary_key_constraint_name)) &.{} else columns;
    return try uniqueConstraintKeysTupleValueFromCellsAlloc(alloc, row_value, cells, constraint.columns, constraint.expressions, constraint.nulls_not_distinct, key_columns);
}

pub fn orderedTupleValueForIndexKeysAlloc(
    alloc: Allocator,
    row_value: []const u8,
    index_keys: []const schema_mod.RelationalIndexKey,
    columns: []const schema_mod.RelationalColumn,
) ![]u8 {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    return try orderedTupleValueForIndexKeyCellsAlloc(alloc, row.cells, index_keys, columns);
}

fn orderedTupleValueForIndexKeyCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    index_keys: []const schema_mod.RelationalIndexKey,
    columns: []const schema_mod.RelationalColumn,
) ![]u8 {
    if (index_keys.len == 0) return error.InvalidColumnValue;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (index_keys) |index_key| {
        const column = findRelationalColumn(columns, index_key.column) orelse return error.InvalidColumnValue;
        var cell = findCellInRow(cells, column.path);
        if (cell == null) cell = findCellInRow(cells, column.name);
        try appendOrderedTupleComponent(alloc, &out, cell, column, index_key);
    }
    return try out.toOwnedSlice(alloc);
}

fn orderedTupleIncludePayloadForCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    include_columns: []const []const u8,
    columns: []const schema_mod.RelationalColumn,
    row_generation: u64,
    index_generation: u64,
    schema_fingerprint: ?[]const u8,
) ![]u8 {
    if (include_columns.len == 0) return try alloc.dupe(u8, "");
    const payload_cells = try alloc.alloc(relational_row_codec.Cell, include_columns.len);
    defer alloc.free(payload_cells);
    var count: usize = 0;
    for (include_columns) |field| {
        const column = findRelationalColumn(columns, field) orelse return error.InvalidColumnValue;
        var cell = findCellInRow(cells, column.path);
        if (cell == null) cell = findCellInRow(cells, column.name);
        if (cell) |found| {
            payload_cells[count] = found;
            count += 1;
        }
    }
    const cell_payload = try relational_row_codec.serialize(alloc, payload_cells[0..count]);
    errdefer alloc.free(cell_payload);
    if (row_generation == 0 or index_generation == 0 or schema_fingerprint == null or schema_fingerprint.?.len == 0) return cell_payload;
    const encoded = try orderedTuplePayloadEnvelopeAlloc(alloc, row_generation, index_generation, schema_fingerprint.?, cell_payload);
    alloc.free(cell_payload);
    return encoded;
}

const ordered_tuple_payload_magic = "\x00antfly:ordered-tuple-payload:v1";

pub const OrderedTuplePayloadView = struct {
    has_metadata: bool = false,
    row_generation: u64 = 0,
    index_generation: u64 = 0,
    schema_fingerprint: []const u8 = "",
    cells: []const u8 = "",
};

fn orderedTuplePayloadEnvelopeAlloc(
    alloc: Allocator,
    row_generation: u64,
    index_generation: u64,
    schema_fingerprint: []const u8,
    cell_payload: []const u8,
) ![]u8 {
    if (schema_fingerprint.len > std.math.maxInt(u16)) return error.InvalidColumnValue;
    var out = try alloc.alloc(u8, ordered_tuple_payload_magic.len + 8 + 8 + 2 + schema_fingerprint.len + cell_payload.len);
    var pos: usize = 0;
    @memcpy(out[pos..][0..ordered_tuple_payload_magic.len], ordered_tuple_payload_magic);
    pos += ordered_tuple_payload_magic.len;
    std.mem.writeInt(u64, out[pos..][0..8], row_generation, .big);
    pos += 8;
    std.mem.writeInt(u64, out[pos..][0..8], index_generation, .big);
    pos += 8;
    std.mem.writeInt(u16, out[pos..][0..2], @intCast(schema_fingerprint.len), .big);
    pos += 2;
    @memcpy(out[pos..][0..schema_fingerprint.len], schema_fingerprint);
    pos += schema_fingerprint.len;
    @memcpy(out[pos..][0..cell_payload.len], cell_payload);
    return out;
}

pub fn orderedTuplePayloadView(entry_payload: []const u8) !OrderedTuplePayloadView {
    if (entry_payload.len == 0) return .{};
    if (!std.mem.startsWith(u8, entry_payload, ordered_tuple_payload_magic)) {
        return .{ .cells = entry_payload };
    }
    var pos: usize = ordered_tuple_payload_magic.len;
    if (entry_payload.len < pos + 8 + 8 + 2) return error.InvalidColumnValue;
    const row_generation = std.mem.readInt(u64, entry_payload[pos..][0..8], .big);
    pos += 8;
    const index_generation = std.mem.readInt(u64, entry_payload[pos..][0..8], .big);
    pos += 8;
    const fingerprint_len = std.mem.readInt(u16, entry_payload[pos..][0..2], .big);
    pos += 2;
    if (entry_payload.len < pos + fingerprint_len) return error.InvalidColumnValue;
    const schema_fingerprint = entry_payload[pos..][0..fingerprint_len];
    pos += fingerprint_len;
    return .{
        .has_metadata = true,
        .row_generation = row_generation,
        .index_generation = index_generation,
        .schema_fingerprint = schema_fingerprint,
        .cells = entry_payload[pos..],
    };
}

pub fn orderedTuplePayloadMatchesMetadata(
    entry_payload: []const u8,
    index: schema_mod.RelationalIndex,
    row_generation: u64,
) !?[]const u8 {
    const view = try orderedTuplePayloadView(entry_payload);
    if (!view.has_metadata) return null;
    if (view.row_generation != row_generation) return null;
    if (view.index_generation != index.generation) return null;
    const schema_fingerprint = index.schema_fingerprint orelse return null;
    if (!std.mem.eql(u8, view.schema_fingerprint, schema_fingerprint)) return null;
    return view.cells;
}

fn orderedTupleEntryRowMatchForRowValueAlloc(
    alloc: Allocator,
    encoded_tuple: []const u8,
    entry_payload: []const u8,
    row_value: []const u8,
    index_keys: []const schema_mod.RelationalIndexKey,
    include_columns: []const []const u8,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleEntryRowMatch {
    const key_count = index_keys.len;
    if (key_count == 0) return error.InvalidColumnValue;
    const include_count = include_columns.len;
    const lookup_count = key_count + include_count;

    var stack_lookups: [64]relational_row_codec.CellLookup = undefined;
    const lookups = if (lookup_count <= stack_lookups.len)
        stack_lookups[0..lookup_count]
    else
        try alloc.alloc(relational_row_codec.CellLookup, lookup_count);
    defer if (lookup_count > stack_lookups.len) alloc.free(lookups);

    var stack_cells: [64]?relational_row_codec.Cell = undefined;
    const cells = if (lookup_count <= stack_cells.len)
        stack_cells[0..lookup_count]
    else
        try alloc.alloc(?relational_row_codec.Cell, lookup_count);
    defer if (lookup_count > stack_cells.len) alloc.free(cells);

    var stack_key_columns: [32]schema_mod.RelationalColumn = undefined;
    const key_columns = if (key_count <= stack_key_columns.len)
        stack_key_columns[0..key_count]
    else
        try alloc.alloc(schema_mod.RelationalColumn, key_count);
    defer if (key_count > stack_key_columns.len) alloc.free(key_columns);

    for (index_keys, 0..) |index_key, i| {
        const column = findRelationalColumn(columns, index_key.column) orelse return error.InvalidColumnValue;
        key_columns[i] = column;
        lookups[i] = relationalRowCellLookupForColumn(column);
    }
    for (include_columns, 0..) |field, i| {
        const column = findRelationalColumn(columns, field) orelse return error.InvalidColumnValue;
        lookups[key_count + i] = relationalRowCellLookupForColumn(column);
    }

    try relational_row_codec.collectCellsByLookup(row_value, lookups, cells);

    const tuple = try orderedTupleValueForMatchedIndexCellsAlloc(alloc, cells[0..key_count], key_columns, index_keys);
    defer alloc.free(tuple);
    if (!std.mem.eql(u8, encoded_tuple, tuple)) return .none;

    if (include_count == 0) {
        return if (entry_payload.len == 0) .tuple_and_payload else .tuple;
    }

    var stack_entry_payload_cells: [32]?relational_row_codec.Cell = undefined;
    const entry_payload_cells = if (include_count <= stack_entry_payload_cells.len)
        stack_entry_payload_cells[0..include_count]
    else
        try alloc.alloc(?relational_row_codec.Cell, include_count);
    defer if (include_count > stack_entry_payload_cells.len) alloc.free(entry_payload_cells);

    const payload_view = try orderedTuplePayloadView(entry_payload);
    try relational_row_codec.collectCellsByLookup(payload_view.cells, lookups[key_count..], entry_payload_cells);
    return if (orderedTupleIncludePayloadCellsMatch(cells[key_count..], entry_payload_cells)) .tuple_and_payload else .tuple;
}

fn orderedTupleIncludePayloadCellsMatch(
    row_cells: []const ?relational_row_codec.Cell,
    entry_payload_cells: []const ?relational_row_codec.Cell,
) bool {
    if (row_cells.len != entry_payload_cells.len) return false;
    for (row_cells, entry_payload_cells) |row_cell, entry_payload_cell| {
        if (row_cell == null and entry_payload_cell == null) continue;
        if (row_cell == null or entry_payload_cell == null) return false;
        if (!relationalCellsEqual(row_cell.?, entry_payload_cell.?)) return false;
    }
    return true;
}

fn relationalRowCellLookupForColumn(column: schema_mod.RelationalColumn) relational_row_codec.CellLookup {
    return .{
        .path = column.path,
        .alternate_path = if (std.mem.eql(u8, column.path, column.name)) "" else column.name,
    };
}

fn orderedTupleValueForMatchedIndexCellsAlloc(
    alloc: Allocator,
    cells: []const ?relational_row_codec.Cell,
    columns: []const schema_mod.RelationalColumn,
    index_keys: []const schema_mod.RelationalIndexKey,
) ![]u8 {
    if (cells.len != index_keys.len or columns.len != index_keys.len) return error.InvalidArgument;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (index_keys, 0..) |index_key, i| {
        try appendOrderedTupleComponent(alloc, &out, cells[i], columns[i], index_key);
    }
    return try out.toOwnedSlice(alloc);
}

pub const OrderedTupleEntryRowMatch = enum {
    none,
    tuple,
    tuple_and_payload,
};

pub fn orderedTupleEntryRowMatchAlloc(
    alloc: Allocator,
    entry_key: []const u8,
    entry_payload: []const u8,
    row_value: []const u8,
    index_column: schema_mod.RelationalColumn,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleEntryRowMatch {
    var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, entry_key)) orelse return .none;
    defer decoded.deinit(alloc);
    if (!std.mem.eql(u8, decoded.index_id, orderedTupleIndexId(index_column))) return .none;

    return try orderedTupleEntryRowMatchForRowValueAlloc(alloc, decoded.encoded_tuple, entry_payload, row_value, index_column.index_keys, index_column.index_include_columns, columns);
}

pub fn orderedTupleEntryRowMatchForIndexAlloc(
    alloc: Allocator,
    entry_key: []const u8,
    entry_payload: []const u8,
    row_value: []const u8,
    index: schema_mod.RelationalIndex,
    columns: []const schema_mod.RelationalColumn,
) !OrderedTupleEntryRowMatch {
    var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, entry_key)) orelse return .none;
    defer decoded.deinit(alloc);
    if (!std.mem.eql(u8, decoded.index_id, index.name)) return .none;

    return try orderedTupleEntryRowMatchForRowValueAlloc(alloc, decoded.encoded_tuple, entry_payload, row_value, index.keys, index.include_columns, columns);
}

pub fn orderedTupleEntryMatchesRowAndPayloadAlloc(
    alloc: Allocator,
    entry_key: []const u8,
    entry_payload: []const u8,
    row_value: []const u8,
    index_column: schema_mod.RelationalColumn,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    return (try orderedTupleEntryRowMatchAlloc(alloc, entry_key, entry_payload, row_value, index_column, columns)) == .tuple_and_payload;
}

fn appendOrderedTupleComponent(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    maybe_cell: ?relational_row_codec.Cell,
    column: schema_mod.RelationalColumn,
    index_key: schema_mod.RelationalIndexKey,
) !void {
    const cell = maybe_cell orelse {
        const tag: [1]u8 = .{if (orderedTupleNullsFirst(index_key)) @as(u8, 0x00) else 0xff};
        try internal_keys.appendEncodedComponent(out, alloc, &tag);
        return;
    };
    if (cell.is_json) return error.InvalidColumnValue;

    var scalar = std.ArrayListUnmanaged(u8).empty;
    defer scalar.deinit(alloc);
    try appendOrderedScalarBytes(alloc, &scalar, cell, column.collation);

    var component = std.ArrayListUnmanaged(u8).empty;
    defer component.deinit(alloc);
    try component.append(alloc, 0x10);
    if (index_key.direction == .desc) {
        var encoded_scalar = std.ArrayListUnmanaged(u8).empty;
        defer encoded_scalar.deinit(alloc);
        try internal_keys.appendEncodedComponent(&encoded_scalar, alloc, scalar.items);
        for (encoded_scalar.items) |byte| try component.append(alloc, ~byte);
    } else {
        try component.appendSlice(alloc, scalar.items);
    }
    try internal_keys.appendEncodedComponent(out, alloc, component.items);
}

fn orderedTupleNullsFirst(index_key: schema_mod.RelationalIndexKey) bool {
    return switch (index_key.nulls) {
        .first => true,
        .last => false,
        .default => index_key.direction == .desc,
    };
}

fn appendOrderedScalarBytes(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    cell: relational_row_codec.Cell,
    collation: ?[]const u8,
) !void {
    switch (cell.value_type) {
        .u64_val => {
            const start = out.items.len;
            try out.resize(alloc, start + @sizeOf(u64));
            std.mem.writeInt(u64, out.items[start..][0..@sizeOf(u64)], cell.value.u64_val, .big);
        },
        .i64_val => {
            const start = out.items.len;
            try out.resize(alloc, start + @sizeOf(u64));
            const sortable_bits: u64 = @as(u64, @bitCast(cell.value.i64_val)) ^ 0x8000_0000_0000_0000;
            std.mem.writeInt(u64, out.items[start..][0..@sizeOf(u64)], sortable_bits, .big);
        },
        .f64_val => {
            const start = out.items.len;
            try out.resize(alloc, start + @sizeOf(u64));
            std.mem.writeInt(u64, out.items[start..][0..@sizeOf(u64)], sortableF64Bits(cell.value.f64_val), .big);
        },
        .bool_val => try out.append(alloc, if (cell.value.bool_val) 1 else 0),
        .bytes_val => {
            const bytes = cell.value.bytes_val;
            if (collation) |name| {
                if (relational_collation.isCaseInsensitive(name)) {
                    for (bytes) |byte| try out.append(alloc, std.ascii.toLower(byte));
                    return;
                }
            }
            try out.appendSlice(alloc, bytes);
        },
        .geo_point => return error.InvalidColumnValue,
    }
}

fn sortableF64Bits(value: f64) u64 {
    const bits: u64 = @bitCast(value);
    return if ((bits & (@as(u64, 1) << 63)) != 0) ~bits else bits ^ (@as(u64, 1) << 63);
}

pub const WriteParticipant = struct {
    const PendingForeignKeyParentCheck = struct {
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        ordered_parent_tuple: ?[]const u8 = null,
        child_span: ?PeriodSpan = null,
    };

    const PendingForeignKeyReferenceAbsenceCheck = struct {
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
    column_index_policy: ColumnIndexPolicy = ColumnIndexPolicy.empty(),
    table_name: []const u8 = "",
    foreign_keys: []const schema_mod.ForeignKey = &.{},
    primary_key: ?schema_mod.PrimaryKey = null,
    unique_constraints: []const schema_mod.UniqueConstraint = &.{},
    relational_columns: []const schema_mod.RelationalColumn = &.{},
    periods: []const schema_mod.RelationalPeriod = &.{},
    planned_delete_keys: []const []const u8 = &.{},
    externalized_parent_checks: []const ExternalizedForeignKeyParentCheck = &.{},
    constraint_timing_overrides: []const ForeignKeyConstraintTimingOverride = &.{},
    pending_fk_parent_checks: std.ArrayListUnmanaged(PendingForeignKeyParentCheck) = .empty,
    pending_fk_reference_absence_checks: std.ArrayListUnmanaged(PendingForeignKeyReferenceAbsenceCheck) = .empty,
    set_null_update_count: usize = 0,
    set_null_update_limit: usize = default_max_set_null_updates,
    cascade_depth: usize = 0,
    cascade_delete_count: usize = 0,
    next_row_generation: u64 = 0,
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
    ) void {
        self.table_name = table_name;
        self.foreign_keys = foreign_keys;
        self.planned_delete_keys = planned_delete_keys;
        self.externalized_parent_checks = &.{};
    }

    pub fn configureExternalizedForeignKeyParentChecks(
        self: *WriteParticipant,
        externalized_parent_checks: []const ExternalizedForeignKeyParentCheck,
    ) void {
        self.externalized_parent_checks = externalized_parent_checks;
    }

    pub fn configureForeignKeyConstraintTimingOverrides(
        self: *WriteParticipant,
        constraint_timing_overrides: []const ForeignKeyConstraintTimingOverride,
    ) void {
        self.constraint_timing_overrides = constraint_timing_overrides;
    }

    pub fn configureUniqueConstraints(
        self: *WriteParticipant,
        unique_constraints: []const schema_mod.UniqueConstraint,
    ) void {
        self.unique_constraints = unique_constraints;
    }

    pub fn configurePrimaryKey(self: *WriteParticipant, primary_key: ?schema_mod.PrimaryKey) void {
        self.primary_key = primary_key;
    }

    pub fn configurePeriods(self: *WriteParticipant, periods: []const schema_mod.RelationalPeriod, relational_columns: []const schema_mod.RelationalColumn) void {
        self.periods = periods;
        self.relational_columns = relational_columns;
    }

    pub fn configureNextRowGeneration(self: *WriteParticipant, row_generation: u64) void {
        self.next_row_generation = row_generation;
    }

    fn uniqueConstraintTupleValueForRowAlloc(
        self: *const WriteParticipant,
        row_value: []const u8,
        constraint: schema_mod.UniqueConstraint,
    ) !?[]u8 {
        return try uniqueConstraintTupleValueWithColumnsAlloc(self.alloc, row_value, constraint, self.relational_columns);
    }

    fn uniqueConstraintTupleValueForCellsAlloc(
        self: *const WriteParticipant,
        row_value: []const u8,
        cells: []const relational_row_codec.Cell,
        constraint: schema_mod.UniqueConstraint,
    ) !?[]u8 {
        return try uniqueConstraintTupleValueWithCellsAlloc(self.alloc, row_value, cells, constraint, self.relational_columns);
    }

    pub fn prepareUpsert(
        self: *WriteParticipant,
        table: []const u8,
        doc_key: []const u8,
        typed_row: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) anyerror!void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        const row_generation = self.next_row_generation;
        self.next_row_generation = 0;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key);
        defer if (old_row) |row| self.alloc.free(row);
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        const needs_primary_cells = if (self.primary_key) |primary_key| primary_key.without_overlaps_period == null else false;
        const needs_unique_cells = self.hasNonTemporalUniqueConstraints();
        const needs_ordered_tuple_unique_cells = self.hasMaintainedOrderedTupleUniqueConstraints();
        const needs_foreign_key_cells = self.hasEnforcedForeignKeys();
        const needs_new_cells = self.periods.len != 0 or
            needs_primary_cells or
            (needs_unique_cells and !final_state_deleted) or
            (needs_ordered_tuple_unique_cells and !final_state_deleted) or
            (needs_foreign_key_cells and !final_state_deleted) or
            self.column_index_policy.mayIndexAnyRow();
        const needs_old_cells = old_row != null and
            (needs_primary_cells or needs_unique_cells or needs_ordered_tuple_unique_cells or needs_foreign_key_cells or self.column_index_policy.mayIndexAnyRow());

        var old_decoded: relational_row_codec.Row = undefined;
        var old_decoded_ready = false;
        defer if (old_decoded_ready) old_decoded.deinit(self.alloc);
        if (needs_old_cells) {
            old_decoded = try relational_row_codec.deserialize(self.alloc, old_row.?);
            old_decoded_ready = true;
        }
        var new_decoded: relational_row_codec.Row = undefined;
        var new_decoded_ready = false;
        defer if (new_decoded_ready) new_decoded.deinit(self.alloc);
        if (needs_new_cells) {
            new_decoded = try relational_row_codec.deserialize(self.alloc, typed_row);
            new_decoded_ready = true;
        }
        const old_cells = if (old_decoded_ready) old_decoded.cells else null;
        const new_cells = if (new_decoded_ready) new_decoded.cells else null;

        try self.validatePeriodBoundsForCells(new_cells orelse &.{});
        try self.preparePrimaryKeyUpsertWithCells(doc_key, old_row, old_cells, typed_row, new_cells, final_state_deleted);
        try self.prepareUniqueConstraintUpsertWithCells(doc_key, old_row, old_cells, typed_row, new_cells, final_state_deleted);
        try self.prepareForeignKeyUpsertWithCells(doc_key, old_row, old_cells, typed_row, new_cells, final_state_deleted);
        try appendUpsertWithColumnIndexPolicyAndOldRowAndCells(self.alloc, self.writes, self.deletes, self.owned_keys, self.owned_values, doc_key, old_row, old_cells, typed_row, new_cells, self.column_index_policy, row_generation);
        try self.appendOrderedTupleUniqueConstraintUpsertWithCells(doc_key, old_row, old_cells, typed_row, new_cells, final_state_deleted);
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
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key);
        defer if (old_row) |row| self.alloc.free(row);
        const needs_old_cells = old_row != null;
        var old_decoded: relational_row_codec.Row = undefined;
        var old_decoded_ready = false;
        defer if (old_decoded_ready) old_decoded.deinit(self.alloc);
        if (needs_old_cells) {
            old_decoded = try relational_row_codec.deserialize(self.alloc, old_row.?);
            old_decoded_ready = true;
        }
        const old_cells = if (old_decoded_ready) old_decoded.cells else null;

        try appendDeleteWithColumnIndexPolicyAndOldRowAndCells(self.alloc, self.deletes, self.owned_keys, doc_key, old_row, old_cells, self.column_index_policy);
        if (old_row) |row| {
            try self.prepareForeignKeyDeleteWithCells(doc_key, row, old_cells.?);
            try self.preparePrimaryKeyDeleteWithCells(doc_key, row, old_cells.?);
            try self.prepareUniqueConstraintDeleteWithCells(doc_key, row, old_cells.?);
            try self.appendOrderedTupleUniqueConstraintDeleteWithCells(doc_key, row, old_cells.?);
        }
        self.prepared = true;
    }

    pub fn prepareIdentityRewrite(
        self: *WriteParticipant,
        table: []const u8,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        new_row: []const u8,
        txn_id: ?transactions_mod.TxnId,
    ) anyerror!void {
        _ = table;
        _ = txn_id;
        if (self.closed) return error.ParticipantClosed;
        if (std.mem.eql(u8, old_doc_key, new_doc_key)) return error.UnsupportedOperation;
        const row_generation = self.next_row_generation;
        self.next_row_generation = 0;
        const old_row = try getRawAlloc(self.alloc, self.store, old_doc_key) orelse return error.RowSelectorNotFound;
        defer self.alloc.free(old_row);
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        var new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
        defer new_decoded.deinit(self.alloc);
        try self.validatePeriodBoundsForCells(new_decoded.cells);

        try self.preparePrimaryKeyIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
        try self.prepareUniqueConstraintIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
        try self.prepareForeignKeyIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
        try self.appendOrderedTupleUniqueConstraintIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
        try appendDeleteWithColumnIndexPolicyAndOldRowAndCells(self.alloc, self.deletes, self.owned_keys, old_doc_key, old_row, old_decoded.cells, ColumnIndexPolicy.empty());

        const existing_new_row = try getRawAlloc(self.alloc, self.store, new_doc_key);
        defer if (existing_new_row) |row| self.alloc.free(row);
        var existing_new_decoded: relational_row_codec.Row = undefined;
        var existing_new_decoded_ready = false;
        defer if (existing_new_decoded_ready) existing_new_decoded.deinit(self.alloc);
        if (existing_new_row != null and self.column_index_policy.mayIndexAnyRow()) {
            existing_new_decoded = try relational_row_codec.deserialize(self.alloc, existing_new_row.?);
            existing_new_decoded_ready = true;
        }
        try appendUpsertWithColumnIndexPolicyAndOldRowAndCells(
            self.alloc,
            self.writes,
            self.deletes,
            self.owned_keys,
            self.owned_values,
            new_doc_key,
            existing_new_row,
            if (existing_new_decoded_ready) existing_new_decoded.cells else null,
            new_row,
            new_decoded.cells,
            self.column_index_policy,
            row_generation,
        );
        self.prepared = true;
    }

    pub fn commit(self: *WriteParticipant, txn_id: ?transactions_mod.TxnId, commit_version: u64) !void {
        _ = txn_id;
        _ = commit_version;
        if (self.closed) return error.ParticipantClosed;
        try self.validatePendingForeignKeyParentChecks();
        try self.validatePendingForeignKeyReferenceAbsenceChecks();
        self.clearPendingForeignKeyParentChecks();
        self.clearPendingForeignKeyReferenceAbsenceChecks();
        self.closed = true;
    }

    pub fn closePreparedIntents(self: *WriteParticipant) !void {
        if (self.closed) return error.ParticipantClosed;
        self.clearPendingForeignKeyParentChecks();
        self.clearPendingForeignKeyReferenceAbsenceChecks();
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
        self.clearPendingForeignKeyReferenceAbsenceChecks();
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
        if (!self.column_index_policy.readyForQuery(column_path)) return error.RelationalIndexNotReady;
        return try scanColumnAlloc(self.alloc, self.store, column_path, lower_doc_key, upper_doc_key);
    }

    fn effectiveTableName(self: *const WriteParticipant) []const u8 {
        return if (self.table_name.len > 0) self.table_name else "_default";
    }

    fn effectiveForeignKeyTiming(self: *const WriteParticipant, foreign_key: schema_mod.ForeignKey) schema_mod.ForeignKeyTiming {
        for (self.constraint_timing_overrides) |override| {
            if (std.mem.eql(u8, override.constraint_name, foreign_key.name)) return override.timing;
        }
        return foreign_key.timing;
    }

    fn foreignKeyDefersNoActionUpdate(self: *const WriteParticipant, foreign_key: schema_mod.ForeignKey) bool {
        return foreign_key.on_update == .no_action and self.effectiveForeignKeyTiming(foreign_key) == .deferred;
    }

    fn foreignKeyDefersNoActionDelete(self: *const WriteParticipant, foreign_key: schema_mod.ForeignKey) bool {
        return foreign_key.on_delete == .no_action and self.effectiveForeignKeyTiming(foreign_key) == .deferred;
    }

    fn hasNonTemporalUniqueConstraints(self: *const WriteParticipant) bool {
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period == null) return true;
        }
        return false;
    }

    fn hasMaintainedOrderedTupleUniqueConstraints(self: *const WriteParticipant) bool {
        for (self.unique_constraints) |constraint| {
            const index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse continue;
            if (orderedTupleUniqueIndexMaintained(constraint, index)) return true;
        }
        return false;
    }

    fn hasEnforcedForeignKeys(self: *const WriteParticipant) bool {
        for (self.foreign_keys) |foreign_key| {
            if (foreignKeyIsEnforced(foreign_key)) return true;
        }
        return false;
    }

    fn validatePeriodBounds(self: *WriteParticipant, row_value: []const u8) !void {
        if (self.periods.len == 0) return;
        var row = try relational_row_codec.deserialize(self.alloc, row_value);
        defer row.deinit(self.alloc);
        try self.validatePeriodBoundsForCells(row.cells);
    }

    fn validatePeriodBoundsForCells(
        self: *WriteParticipant,
        cells: []const relational_row_codec.Cell,
    ) !void {
        if (self.periods.len == 0) return;
        for (self.periods) |period| {
            if (!periodStartBeforeEnd(self.relational_columns, cells, period)) return error.InvalidColumnValue;
        }
    }

    fn prepareUniqueConstraintUpsert(self: *WriteParticipant, doc_key: []const u8, old_row: ?[]const u8, new_row: []const u8) !void {
        if (self.unique_constraints.len == 0) return;
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        var needs_cells = false;
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period == null) {
                needs_cells = true;
                break;
            }
        }
        var old_decoded: relational_row_codec.Row = undefined;
        var old_decoded_ready = false;
        defer if (old_decoded_ready) old_decoded.deinit(self.alloc);
        if (needs_cells) {
            if (old_row) |row| {
                old_decoded = try relational_row_codec.deserialize(self.alloc, row);
                old_decoded_ready = true;
            }
        }
        var new_decoded: relational_row_codec.Row = undefined;
        var new_decoded_ready = false;
        defer if (new_decoded_ready) new_decoded.deinit(self.alloc);
        if (needs_cells and !final_state_deleted) {
            new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
            new_decoded_ready = true;
        }

        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period != null) {
                try self.prepareTemporalUniqueConstraintUpsert(constraint, doc_key, old_row, new_row, final_state_deleted);
                continue;
            }
            const old_base_value = if (old_row) |row| try self.uniqueConstraintTupleValueForCellsAlloc(row, old_decoded.cells, constraint) else null;
            defer if (old_base_value) |value| self.alloc.free(value);
            const new_base_value = if (final_state_deleted) null else try self.uniqueConstraintTupleValueForCellsAlloc(new_row, new_decoded.cells, constraint);
            defer if (new_base_value) |value| self.alloc.free(value);
            if (old_base_value == null and new_base_value == null) continue;
            const enforcement_index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse {
                try self.prepareDedicatedUniqueConstraintChange(constraint, doc_key, old_base_value, new_base_value, new_row);
                continue;
            };
            if (!orderedTupleUniqueIndexLookupReady(constraint, enforcement_index)) return error.RelationalIndexNotReady;
            const old_value = if (old_row) |row| try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, row, old_decoded.cells, constraint, enforcement_index, self.relational_columns) else null;
            defer if (old_value) |value| self.alloc.free(value);
            const new_value = if (final_state_deleted) null else try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, new_row, new_decoded.cells, constraint, enforcement_index, self.relational_columns);
            defer if (new_value) |value| self.alloc.free(value);
            if (optionalBytesEqual(old_value, new_value)) continue;
            var new_value_written = false;
            if (old_value != null) {
                if (new_value != null) {
                    try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, doc_key, new_decoded.cells);
                    new_value_written = true;
                }
            }
            if (old_value) |value| {
                try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
                if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
                try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
            }
            if (new_value != null) {
                if (new_value_written) continue;
                try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, doc_key, new_decoded.cells);
            }
        }
    }

    fn prepareUniqueConstraintUpsertWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: ?[]const u8,
        old_cells: ?[]const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: ?[]const relational_row_codec.Cell,
        final_state_deleted: bool,
    ) !void {
        if (self.unique_constraints.len == 0) return;

        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period != null) {
                try self.prepareTemporalUniqueConstraintUpsert(constraint, doc_key, old_row, new_row, final_state_deleted);
                continue;
            }
            const old_base_value = if (old_row) |row| try self.uniqueConstraintTupleValueForCellsAlloc(row, old_cells.?, constraint) else null;
            defer if (old_base_value) |value| self.alloc.free(value);
            const new_base_value = if (final_state_deleted) null else try self.uniqueConstraintTupleValueForCellsAlloc(new_row, new_cells.?, constraint);
            defer if (new_base_value) |value| self.alloc.free(value);
            if (old_base_value == null and new_base_value == null) continue;
            const enforcement_index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse {
                try self.prepareDedicatedUniqueConstraintChange(constraint, doc_key, old_base_value, new_base_value, new_row);
                continue;
            };
            if (!orderedTupleUniqueIndexLookupReady(constraint, enforcement_index)) return error.RelationalIndexNotReady;
            const old_value = if (old_row) |row| try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, row, old_cells.?, constraint, enforcement_index, self.relational_columns) else null;
            defer if (old_value) |value| self.alloc.free(value);
            const new_value = if (final_state_deleted) null else try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, new_row, new_cells.?, constraint, enforcement_index, self.relational_columns);
            defer if (new_value) |value| self.alloc.free(value);
            if (optionalBytesEqual(old_value, new_value)) continue;
            var new_value_written = false;
            if (old_value != null) {
                if (new_value != null) {
                    try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, doc_key, new_cells.?);
                    new_value_written = true;
                }
            }
            if (old_value) |value| {
                try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
                if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
                try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
            }
            if (new_value != null) {
                if (new_value_written) continue;
                try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, doc_key, new_cells.?);
            }
        }
    }

    fn prepareDedicatedUniqueConstraintChange(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        doc_key: []const u8,
        old_value: ?[]const u8,
        new_value: ?[]const u8,
        new_row: []const u8,
    ) !void {
        if (optionalBytesEqual(old_value, new_value)) return;
        var new_value_written = false;
        if (old_value != null) {
            if (new_value) |value| {
                try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
                try self.appendUniqueConstraintWrite(constraint, value, doc_key);
                new_value_written = true;
            }
        }
        if (old_value) |value| {
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
            if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
            try self.appendUniqueConstraintDelete(constraint, value);
        }
        if (new_value) |value| {
            if (new_value_written) return;
            try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
            try self.appendUniqueConstraintWrite(constraint, value, doc_key);
        }
    }

    fn preparePrimaryKeyUpsert(self: *WriteParticipant, doc_key: []const u8, old_row: ?[]const u8, new_row: []const u8) !void {
        const primary_key = self.primary_key orelse return;
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        if (primary_key.without_overlaps_period != null) {
            try self.prepareTemporalPrimaryKeyUpsert(primary_key, constraint, doc_key, old_row, new_row, final_state_deleted);
            return;
        }

        var old_decoded: relational_row_codec.Row = undefined;
        var old_decoded_ready = false;
        defer if (old_decoded_ready) old_decoded.deinit(self.alloc);
        if (old_row) |row| {
            old_decoded = try relational_row_codec.deserialize(self.alloc, row);
            old_decoded_ready = true;
        }
        var new_decoded: relational_row_codec.Row = undefined;
        var new_decoded_ready = false;
        defer if (new_decoded_ready) new_decoded.deinit(self.alloc);
        if (!final_state_deleted) {
            new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
            new_decoded_ready = true;
        }

        const old_value = if (old_row != null) try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_decoded.cells, primary_key) else null;
        defer if (old_value) |value| self.alloc.free(value);
        const new_value = if (final_state_deleted) null else try primaryKeyTupleValueFromCellsAlloc(self.alloc, new_decoded.cells, primary_key);
        defer if (new_value) |value| self.alloc.free(value);
        if (optionalBytesEqual(old_value, new_value)) return;
        var new_value_written = false;
        if (old_value != null) {
            if (new_value) |value| {
                try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
                try self.appendUniqueConstraintWrite(constraint, value, doc_key);
                new_value_written = true;
            }
        }
        if (old_value) |value| {
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
            if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
            try self.appendUniqueConstraintDelete(constraint, value);
        }
        if (new_value) |value| {
            if (new_value_written) return;
            try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
            try self.appendUniqueConstraintWrite(constraint, value, doc_key);
        }
    }

    fn preparePrimaryKeyUpsertWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: ?[]const u8,
        old_cells: ?[]const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: ?[]const relational_row_codec.Cell,
        final_state_deleted: bool,
    ) !void {
        const primary_key = self.primary_key orelse return;
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        if (primary_key.without_overlaps_period != null) {
            try self.prepareTemporalPrimaryKeyUpsert(primary_key, constraint, doc_key, old_row, new_row, final_state_deleted);
            return;
        }

        const old_value = if (old_row != null) try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_cells.?, primary_key) else null;
        defer if (old_value) |value| self.alloc.free(value);
        const new_value = if (final_state_deleted) null else try primaryKeyTupleValueFromCellsAlloc(self.alloc, new_cells.?, primary_key);
        defer if (new_value) |value| self.alloc.free(value);
        if (optionalBytesEqual(old_value, new_value)) return;
        var new_value_written = false;
        if (old_value != null) {
            if (new_value) |value| {
                try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
                try self.appendUniqueConstraintWrite(constraint, value, doc_key);
                new_value_written = true;
            }
        }
        if (old_value) |value| {
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
            if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
            try self.appendUniqueConstraintDelete(constraint, value);
        }
        if (new_value) |value| {
            if (new_value_written) return;
            try self.requireUniqueConstraintAvailable(constraint, value, doc_key);
            try self.appendUniqueConstraintWrite(constraint, value, doc_key);
        }
    }

    fn preparePrimaryKeyIdentityRewrite(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        new_row: []const u8,
    ) !void {
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        var new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
        defer new_decoded.deinit(self.alloc);
        try self.preparePrimaryKeyIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
    }

    fn preparePrimaryKeyIdentityRewriteWithCells(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: []const relational_row_codec.Cell,
    ) !void {
        _ = old_doc_key;
        _ = old_row;
        const primary_key = self.primary_key orelse return;
        if (primary_key.without_overlaps_period != null) return error.UnsupportedOperation;
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        const old_value = try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_cells, primary_key);
        defer self.alloc.free(old_value);
        const new_value = try primaryKeyTupleValueFromCellsAlloc(self.alloc, new_cells, primary_key);
        defer self.alloc.free(new_value);
        if (std.mem.eql(u8, old_value, new_value)) return error.UnsupportedOperation;
        try self.requireUniqueConstraintAvailable(constraint, new_value, new_doc_key);
        try self.appendUniqueConstraintWrite(constraint, new_value, new_doc_key);
        try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, old_value);
        try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, old_value, new_row);
        try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, old_value);
        try self.appendUniqueConstraintDelete(constraint, old_value);
    }

    fn prepareUniqueConstraintIdentityRewrite(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        new_row: []const u8,
    ) !void {
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        var new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
        defer new_decoded.deinit(self.alloc);
        try self.prepareUniqueConstraintIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
    }

    fn prepareUniqueConstraintIdentityRewriteWithCells(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: []const relational_row_codec.Cell,
    ) !void {
        _ = old_doc_key;
        if (self.unique_constraints.len == 0) return;
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period != null) return error.UnsupportedOperation;
            const old_base_value = try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_cells, constraint);
            defer if (old_base_value) |value| self.alloc.free(value);
            const new_base_value = try self.uniqueConstraintTupleValueForCellsAlloc(new_row, new_cells, constraint);
            defer if (new_base_value) |value| self.alloc.free(value);
            if (old_base_value == null and new_base_value == null) continue;
            const enforcement_index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse {
                if (old_base_value == null) {
                    try self.requireUniqueConstraintAvailable(constraint, new_base_value.?, new_doc_key);
                    try self.appendUniqueConstraintWrite(constraint, new_base_value.?, new_doc_key);
                    continue;
                }
                if (new_base_value == null) {
                    try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, old_base_value.?);
                    try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, old_base_value.?);
                    try self.appendUniqueConstraintDelete(constraint, old_base_value.?);
                    continue;
                }
                if (std.mem.eql(u8, old_base_value.?, new_base_value.?)) return error.UnsupportedOperation;
                try self.requireUniqueConstraintAvailable(constraint, new_base_value.?, new_doc_key);
                try self.appendUniqueConstraintWrite(constraint, new_base_value.?, new_doc_key);
                try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, old_base_value.?);
                try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, old_base_value.?, new_row);
                try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, old_base_value.?);
                try self.appendUniqueConstraintDelete(constraint, old_base_value.?);
                continue;
            };
            if (!orderedTupleUniqueIndexLookupReady(constraint, enforcement_index)) return error.RelationalIndexNotReady;
            const old_value = try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, old_row, old_cells, constraint, enforcement_index, self.relational_columns);
            defer if (old_value) |value| self.alloc.free(value);
            const new_value = try orderedTupleUniqueIndexTupleValueForCellsAlloc(self.alloc, new_row, new_cells, constraint, enforcement_index, self.relational_columns);
            defer if (new_value) |value| self.alloc.free(value);
            if (old_value == null and new_value == null) continue;
            if (old_value == null) {
                try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, new_doc_key, new_cells);
                continue;
            }
            if (new_value == null) {
                try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, old_value.?);
                try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, old_value.?);
                continue;
            }
            if (std.mem.eql(u8, old_value.?, new_value.?)) continue;
            try self.requireOrderedTupleUniqueConstraintAvailableForCells(constraint, enforcement_index, new_doc_key, new_cells);
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, old_value.?);
            try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, old_value.?, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, old_value.?);
        }
    }

    fn preparePrimaryKeyDelete(self: *WriteParticipant, doc_key: []const u8) !void {
        const primary_key = self.primary_key orelse return;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key) orelse return;
        defer self.alloc.free(old_row);
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        const value = try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_decoded.cells, primary_key);
        defer self.alloc.free(value);
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        if (primary_key.without_overlaps_period) |period_name| {
            const span = try self.periodSpanForRow(old_row, period_name);
            try self.appendTemporalUniqueConstraintDelete(constraint, value, span, doc_key);
        } else {
            try self.appendUniqueConstraintDelete(constraint, value);
        }
    }

    fn preparePrimaryKeyDeleteWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
    ) !void {
        _ = old_row;
        const primary_key = self.primary_key orelse return;
        const value = try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_cells, primary_key);
        defer self.alloc.free(value);
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        if (primary_key.without_overlaps_period) |period_name| {
            const span = try self.periodSpanForCells(old_cells, period_name);
            try self.appendTemporalUniqueConstraintDelete(constraint, value, span, doc_key);
        } else {
            try self.appendUniqueConstraintDelete(constraint, value);
        }
    }

    fn appendOrderedTupleUniqueConstraintUpsertWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: ?[]const u8,
        old_cells: ?[]const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: ?[]const relational_row_codec.Cell,
        final_state_deleted: bool,
    ) !void {
        if (self.unique_constraints.len == 0) return;
        for (self.unique_constraints) |constraint| {
            const index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse continue;
            if (!orderedTupleUniqueIndexMaintained(constraint, index)) continue;
            const old_indexed = if (old_row != null)
                try self.rowMatchesOrderedTupleUniqueIndex(old_row.?, old_cells.?, index)
            else
                false;
            const new_indexed = if (!final_state_deleted)
                try self.rowMatchesOrderedTupleUniqueIndex(new_row, new_cells.?, index)
            else
                false;

            var old_keys_maybe = if (old_indexed)
                try orderedTupleUniqueIndexEntryKeysForCellsAlloc(self.alloc, constraint, index, doc_key, old_row.?, old_cells.?, self.relational_columns)
            else
                null;
            defer if (old_keys_maybe) |*old_keys| old_keys.deinit(self.alloc);
            var new_keys_maybe = if (new_indexed)
                try orderedTupleUniqueIndexEntryKeysForCellsAlloc(self.alloc, constraint, index, doc_key, new_row, new_cells.?, self.relational_columns)
            else
                null;
            defer if (new_keys_maybe) |*new_keys| new_keys.deinit(self.alloc);

            if (old_keys_maybe) |*old_keys| {
                if (new_keys_maybe) |*new_keys| {
                    if (std.mem.eql(u8, old_keys.encoded_tuple, new_keys.encoded_tuple)) {
                        if (!std.mem.eql(u8, old_keys.forward_value, new_keys.forward_value)) {
                            try appendOrderedTupleForwardIndexWriteKey(self.alloc, self.writes, self.owned_keys, self.owned_values, new_keys);
                        }
                        continue;
                    }
                    try appendOrderedTupleIndexDeleteKeys(self.alloc, self.deletes, self.owned_keys, old_keys);
                    try appendOrderedTupleIndexWriteKeys(self.alloc, self.writes, self.owned_keys, self.owned_values, new_keys);
                } else {
                    try appendOrderedTupleIndexDeleteKeys(self.alloc, self.deletes, self.owned_keys, old_keys);
                }
            } else if (new_keys_maybe) |*new_keys| {
                try appendOrderedTupleIndexWriteKeys(self.alloc, self.writes, self.owned_keys, self.owned_values, new_keys);
            }
        }
    }

    fn appendOrderedTupleUniqueConstraintDeleteWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
    ) !void {
        if (self.unique_constraints.len == 0) return;
        for (self.unique_constraints) |constraint| {
            const index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse continue;
            if (!orderedTupleUniqueIndexMaintained(constraint, index)) continue;
            if (!(try self.rowMatchesOrderedTupleUniqueIndex(old_row, old_cells, index))) continue;
            var keys = (try orderedTupleUniqueIndexEntryKeysForCellsAlloc(self.alloc, constraint, index, doc_key, old_row, old_cells, self.relational_columns)) orelse continue;
            defer keys.deinit(self.alloc);
            try appendOrderedTupleIndexDeleteKeys(self.alloc, self.deletes, self.owned_keys, &keys);
        }
    }

    fn appendOrderedTupleUniqueConstraintIdentityRewriteWithCells(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: []const relational_row_codec.Cell,
    ) !void {
        if (self.unique_constraints.len == 0) return;
        for (self.unique_constraints) |constraint| {
            const index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse continue;
            if (!orderedTupleUniqueIndexMaintained(constraint, index)) continue;
            if (try self.rowMatchesOrderedTupleUniqueIndex(old_row, old_cells, index)) {
                var old_keys_maybe = try orderedTupleUniqueIndexEntryKeysForCellsAlloc(self.alloc, constraint, index, old_doc_key, old_row, old_cells, self.relational_columns);
                if (old_keys_maybe) |*old_keys| {
                    defer old_keys.deinit(self.alloc);
                    try appendOrderedTupleIndexDeleteKeys(self.alloc, self.deletes, self.owned_keys, old_keys);
                }
            }
            if (try self.rowMatchesOrderedTupleUniqueIndex(new_row, new_cells, index)) {
                var new_keys_maybe = try orderedTupleUniqueIndexEntryKeysForCellsAlloc(self.alloc, constraint, index, new_doc_key, new_row, new_cells, self.relational_columns);
                if (new_keys_maybe) |*new_keys| {
                    defer new_keys.deinit(self.alloc);
                    try appendOrderedTupleIndexWriteKeys(self.alloc, self.writes, self.owned_keys, self.owned_values, new_keys);
                }
            }
        }
    }

    fn rowMatchesOrderedTupleUniqueIndex(
        self: *WriteParticipant,
        row_value: []const u8,
        cells: []const relational_row_codec.Cell,
        index: schema_mod.RelationalIndex,
    ) !bool {
        if (!(try rowMatchesUniqueConstraintPredicatesFromCells(self.alloc, cells, index.where, self.relational_columns))) return false;
        if (!(try rowMatchesExpressionConditionsWithColumns(self.alloc, row_value, index.where_expressions, self.relational_columns))) return false;
        return true;
    }

    fn prepareUniqueConstraintDelete(self: *WriteParticipant, doc_key: []const u8) !void {
        if (self.unique_constraints.len == 0) return;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key) orelse return;
        defer self.alloc.free(old_row);
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period) |period_name| {
                const value = (try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_decoded.cells, constraint)) orelse continue;
                defer self.alloc.free(value);
                const span = try self.periodSpanForRow(old_row, period_name);
                try self.appendTemporalUniqueConstraintDelete(constraint, value, span, doc_key);
                continue;
            }
            const base_value = (try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_decoded.cells, constraint)) orelse continue;
            defer self.alloc.free(base_value);
            const enforcement_index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse {
                try self.appendUniqueConstraintDelete(constraint, base_value);
                continue;
            };
            if (!orderedTupleUniqueIndexLookupReady(constraint, enforcement_index)) return error.RelationalIndexNotReady;
        }
    }

    fn prepareUniqueConstraintDeleteWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
    ) !void {
        if (self.unique_constraints.len == 0) return;
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            if (constraint.without_overlaps_period) |period_name| {
                const value = (try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_cells, constraint)) orelse continue;
                defer self.alloc.free(value);
                const span = try self.periodSpanForCells(old_cells, period_name);
                try self.appendTemporalUniqueConstraintDelete(constraint, value, span, doc_key);
                continue;
            }
            const base_value = (try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_cells, constraint)) orelse continue;
            defer self.alloc.free(base_value);
            const enforcement_index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse {
                try self.appendUniqueConstraintDelete(constraint, base_value);
                continue;
            };
            if (!orderedTupleUniqueIndexLookupReady(constraint, enforcement_index)) return error.RelationalIndexNotReady;
        }
    }

    fn prepareTemporalPrimaryKeyUpsert(
        self: *WriteParticipant,
        primary_key: schema_mod.PrimaryKey,
        constraint: schema_mod.UniqueConstraint,
        doc_key: []const u8,
        old_row: ?[]const u8,
        new_row: []const u8,
        final_state_deleted: bool,
    ) !void {
        const period_name = primary_key.without_overlaps_period orelse return;
        const old_value = if (old_row) |row| try primaryKeyTupleValueAlloc(self.alloc, row, primary_key) else null;
        defer if (old_value) |value| self.alloc.free(value);
        const old_span = if (old_row != null and old_value != null) try self.periodSpanForRow(old_row.?, period_name) else null;
        const new_value = if (final_state_deleted) null else try primaryKeyTupleValueAlloc(self.alloc, new_row, primary_key);
        defer if (new_value) |value| self.alloc.free(value);
        const new_span = if (new_value != null) try self.periodSpanForRow(new_row, period_name) else null;
        if (optionalBytesEqual(old_value, new_value) and optionalPeriodSpanEqual(old_span, new_span)) return;
        if (new_value) |value| {
            try self.requireTemporalUniqueConstraintAvailable(constraint, value, new_span.?, doc_key);
            try self.appendTemporalUniqueConstraintWrite(constraint, value, new_span.?, doc_key);
        }
        if (old_value) |value| {
            try self.appendTemporalUniqueConstraintDelete(constraint, value, old_span.?, doc_key);
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
            if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
        }
    }

    fn prepareTemporalUniqueConstraintUpsert(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        doc_key: []const u8,
        old_row: ?[]const u8,
        new_row: []const u8,
        final_state_deleted: bool,
    ) !void {
        const period_name = constraint.without_overlaps_period orelse return;
        const old_value = if (old_row) |row| try self.uniqueConstraintTupleValueForRowAlloc(row, constraint) else null;
        defer if (old_value) |value| self.alloc.free(value);
        const old_span = if (old_row != null and old_value != null) try self.periodSpanForRow(old_row.?, period_name) else null;
        const new_value = if (final_state_deleted) null else try self.uniqueConstraintTupleValueForRowAlloc(new_row, constraint);
        defer if (new_value) |value| self.alloc.free(value);
        const new_span = if (new_value != null) try self.periodSpanForRow(new_row, period_name) else null;
        if (optionalBytesEqual(old_value, new_value) and optionalPeriodSpanEqual(old_span, new_span)) return;
        if (new_value) |value| {
            try self.requireTemporalUniqueConstraintAvailable(constraint, value, new_span.?, doc_key);
            try self.appendTemporalUniqueConstraintWrite(constraint, value, new_span.?, doc_key);
        }
        if (old_value) |value| {
            try self.appendTemporalUniqueConstraintDelete(constraint, value, old_span.?, doc_key);
            try self.applySetNullUpdatingUniqueForeignKeyRefs(constraint, value);
            if (new_value != null) try self.applyCascadeUpdatingUniqueForeignKeyRefs(constraint, value, new_row);
            try self.requireNoUpdatingUniqueForeignKeyRefs(constraint, value);
        }
    }

    fn requireOrderedTupleUniqueConstraintAvailableForCells(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        index: schema_mod.RelationalIndex,
        doc_key: []const u8,
        cells: []const relational_row_codec.Cell,
    ) !void {
        const row_value = try relational_row_codec.serialize(self.alloc, cells);
        defer self.alloc.free(row_value);
        var candidate_keys = (try orderedTupleUniqueIndexEntryKeysForCellsAlloc(
            self.alloc,
            constraint,
            index,
            doc_key,
            row_value,
            cells,
            self.relational_columns,
        )) orelse return;
        defer candidate_keys.deinit(self.alloc);

        for (self.writes.items) |write| {
            var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (!std.mem.eql(u8, decoded.index_id, index.name)) continue;
            if (!std.mem.eql(u8, decoded.encoded_tuple, candidate_keys.encoded_tuple)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            if (std.mem.eql(u8, decoded.doc_key, doc_key)) continue;
            if (containsKey(self.planned_delete_keys, decoded.doc_key)) continue;
            return error.UniqueConstraintViolation;
        }

        const committed_docs = try scanOrderedTupleDocKeysAlloc(self.alloc, self.store, index.name, candidate_keys.encoded_tuple, "", "");
        defer freeDocKeys(self.alloc, committed_docs);
        for (committed_docs) |owner_doc_key| {
            if (std.mem.eql(u8, owner_doc_key, doc_key)) continue;
            if (containsKey(self.planned_delete_keys, owner_doc_key)) continue;
            const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(self.alloc, index.name, candidate_keys.encoded_tuple, owner_doc_key);
            defer self.alloc.free(forward_key);
            if (containsBatchDelete(self.deletes.items, forward_key)) continue;
            return error.UniqueConstraintViolation;
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

    fn requireTemporalUniqueConstraintAvailable(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        span: PeriodSpan,
        doc_key: []const u8,
    ) !void {
        const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(self.alloc, constraint.name, encoded_value);
        defer self.alloc.free(prefix);

        for (self.writes.items) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            if (std.mem.eql(u8, write.value, doc_key)) continue;
            const existing_span = try decodeTemporalUniqueSpanFromKeyAlloc(self.alloc, write.key, prefix);
            if (periodSpansOverlap(span, existing_span)) return error.UniqueConstraintViolation;
        }

        const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(self.alloc, constraint.name, encoded_value);
        defer if (upper) |buf| self.alloc.free(buf);
        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            const owner = batchWriteValue(self.writes.items, entry.key) orelse entry.value;
            if (std.mem.eql(u8, owner, doc_key)) continue;
            if (containsKey(self.planned_delete_keys, owner)) continue;
            const existing_span = try decodeTemporalUniqueSpanFromKeyAlloc(self.alloc, entry.key, prefix);
            if (periodSpansOverlap(span, existing_span)) return error.UniqueConstraintViolation;
        }
    }

    fn appendTemporalUniqueConstraintWrite(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        span: PeriodSpan,
        doc_key: []const u8,
    ) !void {
        const key = try temporalUniqueConstraintKeyAlloc(self.alloc, constraint, encoded_value, span, doc_key);
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

    fn appendTemporalUniqueConstraintDelete(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        span: PeriodSpan,
        doc_key: []const u8,
    ) !void {
        const key = try temporalUniqueConstraintKeyAlloc(self.alloc, constraint, encoded_value, span, doc_key);
        var key_owned = true;
        errdefer if (key_owned) self.alloc.free(key);
        try self.owned_keys.append(self.alloc, key);
        key_owned = false;
        try self.deletes.append(self.alloc, key);
    }

    fn periodSpanForRow(self: *WriteParticipant, row_value: []const u8, period_name: []const u8) !PeriodSpan {
        var row = try relational_row_codec.deserialize(self.alloc, row_value);
        defer row.deinit(self.alloc);
        return try self.periodSpanForCells(row.cells, period_name);
    }

    fn periodSpanForCells(
        self: *WriteParticipant,
        cells: []const relational_row_codec.Cell,
        period_name: []const u8,
    ) !PeriodSpan {
        const period = self.findPeriod(period_name) orelse return error.InvalidColumnValue;
        return try periodSpanFromCells(self.relational_columns, cells, period);
    }

    fn findPeriod(self: *const WriteParticipant, name: []const u8) ?schema_mod.RelationalPeriod {
        for (self.periods) |period| {
            if (std.mem.eql(u8, period.name, name)) return period;
        }
        return null;
    }

    fn prepareForeignKeyUpsert(self: *WriteParticipant, doc_key: []const u8, old_row: ?[]const u8, new_row: []const u8) !void {
        if (self.foreign_keys.len == 0) return;
        const final_state_deleted = containsKey(self.planned_delete_keys, doc_key);
        var old_decoded: relational_row_codec.Row = undefined;
        var old_decoded_ready = false;
        defer if (old_decoded_ready) old_decoded.deinit(self.alloc);
        if (old_row) |row| {
            old_decoded = try relational_row_codec.deserialize(self.alloc, row);
            old_decoded_ready = true;
        }
        var new_decoded: relational_row_codec.Row = undefined;
        var new_decoded_ready = false;
        defer if (new_decoded_ready) new_decoded.deinit(self.alloc);
        if (!final_state_deleted) {
            new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
            new_decoded_ready = true;
        }

        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const old_parent = if (old_row != null) try self.foreignKeyReferenceValueForCellsAlloc(old_decoded.cells, foreign_key) else null;
            defer if (old_parent) |value| self.alloc.free(value);
            const new_parent = if (final_state_deleted) null else try self.foreignKeyReferenceValueForCellsAlloc(new_decoded.cells, foreign_key);
            defer if (new_parent) |value| self.alloc.free(value);
            const old_span = if (old_row != null and old_parent != null and foreign_key.child_period != null) try self.periodSpanForCells(old_decoded.cells, foreign_key.child_period.?) else null;
            const new_span = if (!final_state_deleted and new_parent != null and foreign_key.child_period != null) try self.periodSpanForCells(new_decoded.cells, foreign_key.child_period.?) else null;
            const parent_changed = !optionalBytesEqual(old_parent, new_parent);
            if (!parent_changed and optionalPeriodSpanEqual(old_span, new_span)) continue;
            if (old_parent) |parent_key| if (parent_changed) try self.appendForeignKeyRefDelete(foreign_key, parent_key, doc_key);
            if (new_parent) |parent_key| {
                const ordered_parent_tuple = if (foreign_key.child_period == null and foreign_key.parent_period == null)
                    try self.orderedTupleParentValueForForeignKeyCellsAlloc(foreign_key, new_decoded.cells)
                else
                    null;
                defer if (ordered_parent_tuple) |tuple| self.alloc.free(tuple);
                if (!self.parentCheckExternalized(foreign_key, doc_key, parent_key, ordered_parent_tuple, new_span)) try self.deferForeignKeyParentCheck(foreign_key, parent_key, ordered_parent_tuple, new_span);
                if (parent_changed) try self.appendForeignKeyRefWrite(foreign_key, parent_key, doc_key);
            }
        }
    }

    fn prepareForeignKeyUpsertWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: ?[]const u8,
        old_cells: ?[]const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: ?[]const relational_row_codec.Cell,
        final_state_deleted: bool,
    ) !void {
        _ = new_row;
        if (self.foreign_keys.len == 0) return;

        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const old_parent = if (old_row != null) try self.foreignKeyReferenceValueForCellsAlloc(old_cells.?, foreign_key) else null;
            defer if (old_parent) |value| self.alloc.free(value);
            const new_parent = if (final_state_deleted) null else try self.foreignKeyReferenceValueForCellsAlloc(new_cells.?, foreign_key);
            defer if (new_parent) |value| self.alloc.free(value);
            const old_span = if (old_row != null and old_parent != null and foreign_key.child_period != null) try self.periodSpanForCells(old_cells.?, foreign_key.child_period.?) else null;
            const new_span = if (!final_state_deleted and new_parent != null and foreign_key.child_period != null) try self.periodSpanForCells(new_cells.?, foreign_key.child_period.?) else null;
            const parent_changed = !optionalBytesEqual(old_parent, new_parent);
            if (!parent_changed and optionalPeriodSpanEqual(old_span, new_span)) continue;
            if (old_parent) |parent_key| if (parent_changed) try self.appendForeignKeyRefDelete(foreign_key, parent_key, doc_key);
            if (new_parent) |parent_key| {
                const ordered_parent_tuple = if (foreign_key.child_period == null and foreign_key.parent_period == null)
                    try self.orderedTupleParentValueForForeignKeyCellsAlloc(foreign_key, new_cells.?)
                else
                    null;
                defer if (ordered_parent_tuple) |tuple| self.alloc.free(tuple);
                if (!self.parentCheckExternalized(foreign_key, doc_key, parent_key, ordered_parent_tuple, new_span)) try self.deferForeignKeyParentCheck(foreign_key, parent_key, ordered_parent_tuple, new_span);
                if (parent_changed) try self.appendForeignKeyRefWrite(foreign_key, parent_key, doc_key);
            }
        }
    }

    fn prepareForeignKeyIdentityRewrite(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        new_row: []const u8,
    ) !void {
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        var new_decoded = try relational_row_codec.deserialize(self.alloc, new_row);
        defer new_decoded.deinit(self.alloc);
        try self.prepareForeignKeyIdentityRewriteWithCells(old_doc_key, new_doc_key, old_row, old_decoded.cells, new_row, new_decoded.cells);
    }

    fn prepareForeignKeyIdentityRewriteWithCells(
        self: *WriteParticipant,
        old_doc_key: []const u8,
        new_doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
        new_row: []const u8,
        new_cells: []const relational_row_codec.Cell,
    ) !void {
        _ = old_row;
        _ = new_row;
        if (self.foreign_keys.len == 0) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.child_period != null or foreign_key.parent_period != null) return error.UnsupportedOperation;
            const old_parent = try self.foreignKeyReferenceValueForCellsAlloc(old_cells, foreign_key);
            defer if (old_parent) |value| self.alloc.free(value);
            const new_parent = try self.foreignKeyReferenceValueForCellsAlloc(new_cells, foreign_key);
            defer if (new_parent) |value| self.alloc.free(value);
            const parent_changed = !optionalBytesEqual(old_parent, new_parent);
            const child_key_changed = !std.mem.eql(u8, old_doc_key, new_doc_key);
            if (!parent_changed and !child_key_changed) continue;
            if (old_parent) |parent_key| try self.appendForeignKeyRefDelete(foreign_key, parent_key, old_doc_key);
            if (new_parent) |parent_key| {
                const ordered_parent_tuple = try self.orderedTupleParentValueForForeignKeyCellsAlloc(foreign_key, new_cells);
                defer if (ordered_parent_tuple) |tuple| self.alloc.free(tuple);
                if (parent_changed and !self.parentCheckExternalized(foreign_key, new_doc_key, parent_key, ordered_parent_tuple, null)) try self.deferForeignKeyParentCheck(foreign_key, parent_key, ordered_parent_tuple, null);
                try self.appendForeignKeyRefWrite(foreign_key, parent_key, new_doc_key);
            }
        }
    }

    fn parentCheckExternalized(self: *const WriteParticipant, foreign_key: schema_mod.ForeignKey, child_key: []const u8, parent_key: []const u8, ordered_parent_tuple: ?[]const u8, child_span: ?PeriodSpan) bool {
        for (self.externalized_parent_checks) |check| {
            if (!std.mem.eql(u8, check.constraint_name, foreign_key.name)) continue;
            if (!std.mem.eql(u8, check.child_table, self.effectiveTableName())) continue;
            if (!std.mem.eql(u8, check.child_key, child_key)) continue;
            if (!std.mem.eql(u8, check.parent_table, foreign_key.parent_table)) continue;
            if (!std.mem.eql(u8, check.parent_key, parent_key)) continue;
            if (check.timing != self.effectiveForeignKeyTiming(foreign_key)) continue;
            if (foreign_key.child_period != null or foreign_key.parent_period != null) {
                const span = child_span orelse continue;
                if (!externalizedTemporalParentCheckMatchesSpan(self.alloc, check, span)) continue;
            } else if (check.child_period_start_json != null or check.child_period_end_json != null) continue;
            if (foreignKeyReferencesPrimaryKey(foreign_key)) {
                if (check.parent_constraint_name != null) continue;
                if (check.ordered_parent_tuple != null) continue;
            } else {
                const check_constraint_name = check.parent_constraint_name orelse continue;
                if (check_constraint_name.len == 0) continue;
                if (ordered_parent_tuple) |tuple| {
                    const check_tuple = check.ordered_parent_tuple orelse continue;
                    if (!std.mem.eql(u8, tuple, check_tuple)) continue;
                } else if (check.ordered_parent_tuple != null) continue;
                if (std.mem.eql(u8, foreign_key.parent_table, self.effectiveTableName())) {
                    const parent_constraint = self.findParentTupleConstraint(foreign_key.parent_columns) orelse continue;
                    if (!std.mem.eql(u8, check_constraint_name, parent_constraint.name)) continue;
                }
            }
            return true;
        }
        return false;
    }

    fn prepareForeignKeyDelete(self: *WriteParticipant, doc_key: []const u8) anyerror!void {
        if (self.foreign_keys.len == 0) return;
        const old_row = try getRawAlloc(self.alloc, self.store, doc_key) orelse return;
        defer self.alloc.free(old_row);
        var old_decoded = try relational_row_codec.deserialize(self.alloc, old_row);
        defer old_decoded.deinit(self.alloc);
        try self.prepareForeignKeyDeleteWithCells(doc_key, old_row, old_decoded.cells);
    }

    fn prepareForeignKeyDeleteWithCells(
        self: *WriteParticipant,
        doc_key: []const u8,
        old_row: []const u8,
        old_cells: []const relational_row_codec.Cell,
    ) anyerror!void {
        if (self.foreign_keys.len == 0) return;
        const primary_tuple = if (self.primary_key) |primary_key| try primaryKeyTupleValueFromCellsAlloc(self.alloc, old_cells, primary_key) else null;
        defer if (primary_tuple) |value| self.alloc.free(value);
        const primary_constraint = if (self.primary_key) |primary_key| primaryKeyAsUniqueConstraint(primary_key) else null;

        var unique_tuples = std.ArrayListUnmanaged(struct {
            constraint: schema_mod.UniqueConstraint,
            value: []u8,
        }).empty;
        defer {
            for (unique_tuples.items) |tuple| self.alloc.free(tuple.value);
            unique_tuples.deinit(self.alloc);
        }
        for (self.unique_constraints) |constraint| {
            if (!uniqueConstraintIsEnforced(constraint)) continue;
            const value = (try self.uniqueConstraintTupleValueForCellsAlloc(old_row, old_cells, constraint)) orelse continue;
            var value_owned = true;
            errdefer if (value_owned) self.alloc.free(value);
            try unique_tuples.append(self.alloc, .{ .constraint = constraint, .value = value });
            value_owned = false;
        }

        try self.applySetNullPrimaryKeyForeignKeyRefs(doc_key);
        if (primary_constraint) |constraint| {
            try self.applySetNullUniqueForeignKeyRefs(constraint, primary_tuple.?);
        }
        for (unique_tuples.items) |tuple| {
            try self.applySetNullUniqueForeignKeyRefs(tuple.constraint, tuple.value);
        }

        try self.applyCascadePrimaryKeyForeignKeyRefs(doc_key);
        if (primary_constraint) |constraint| {
            try self.applyCascadeUniqueForeignKeyRefs(constraint, primary_tuple.?);
        }
        for (unique_tuples.items) |tuple| {
            try self.applyCascadeUniqueForeignKeyRefs(tuple.constraint, tuple.value);
        }

        try self.requireNoRestrictingPrimaryKeyForeignKeyRefs(doc_key);
        if (primary_constraint) |constraint| {
            try self.requireNoRestrictingUniqueForeignKeyRefs(constraint, primary_tuple.?, old_cells);
        }
        for (unique_tuples.items) |tuple| {
            try self.requireNoRestrictingUniqueForeignKeyRefs(tuple.constraint, tuple.value, old_cells);
        }

        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const parent_key = (try self.foreignKeyReferenceValueForCellsAlloc(old_cells, foreign_key)) orelse continue;
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

    pub fn prepareSetNullForeignKeyUpdateChildAction(
        self: *WriteParticipant,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        child_key: []const u8,
    ) !void {
        const foreign_key = findForeignKeyByName(self.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
        if (!foreignKeyIsEnforced(foreign_key)) return error.ForeignKeyViolation;
        if (foreign_key.on_update != .set_null) return error.ForeignKeyViolation;
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
        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, parent_key)) return;
        try self.prepareCascadeForeignKeyChild(foreign_key, parent_key, child_key);
    }

    pub fn prepareCascadeForeignKeyUpdateChildAction(
        self: *WriteParticipant,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: []const u8,
        child_key: []const u8,
    ) !void {
        const foreign_key = findForeignKeyByName(self.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
        if (!foreignKeyIsEnforced(foreign_key)) return error.ForeignKeyViolation;
        if (foreign_key.on_update != .cascade) return error.ForeignKeyViolation;
        if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
        try self.prepareCascadeUpdateForeignKeyChildFromParentKey(foreign_key, parent_key, updated_parent_key, child_key);
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
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .set_null) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
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

        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, parent_key)) return;
        if (try self.temporalForeignKeyChildRemainsCovered(foreign_key, parent_key, child_key)) return;
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
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_delete != .cascade) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
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
            try self.prepareCascadeForeignKeyChild(foreign_key, parent_key, decoded.child_key);
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareCascadeForeignKeyChild(foreign_key, parent_key, decoded.child_key);
        }
    }

    fn prepareCascadeForeignKeyChild(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        child_key: []const u8,
    ) anyerror!void {
        if (containsKey(self.planned_delete_keys, child_key)) return;
        if (try self.isRowDeletePlanned(child_key)) return;
        if (try self.temporalForeignKeyChildRemainsCovered(foreign_key, parent_key, child_key)) return;
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
        ordered_parent_tuple: ?[]const u8,
        child_span: ?PeriodSpan,
    ) !void {
        const parent_key_owned = try self.alloc.dupe(u8, parent_key);
        var parent_key_transferred = false;
        errdefer if (!parent_key_transferred) self.alloc.free(parent_key_owned);
        const ordered_parent_tuple_owned = if (ordered_parent_tuple) |tuple| try self.alloc.dupe(u8, tuple) else null;
        var ordered_parent_tuple_transferred = false;
        errdefer if (!ordered_parent_tuple_transferred) if (ordered_parent_tuple_owned) |tuple| self.alloc.free(@constCast(tuple));
        try self.pending_fk_parent_checks.append(self.alloc, .{
            .foreign_key = foreign_key,
            .parent_key = parent_key_owned,
            .ordered_parent_tuple = ordered_parent_tuple_owned,
            .child_span = child_span,
        });
        parent_key_transferred = true;
        ordered_parent_tuple_transferred = true;
    }

    fn validatePendingForeignKeyParentChecks(self: *WriteParticipant) !void {
        for (self.pending_fk_parent_checks.items) |check| {
            try self.requireForeignKeyParentExists(check.foreign_key, check.parent_key, check.ordered_parent_tuple, check.child_span);
        }
    }

    fn clearPendingForeignKeyParentChecks(self: *WriteParticipant) void {
        for (self.pending_fk_parent_checks.items) |check| {
            self.alloc.free(@constCast(check.parent_key));
            if (check.ordered_parent_tuple) |tuple| self.alloc.free(@constCast(tuple));
        }
        self.pending_fk_parent_checks.deinit(self.alloc);
        self.pending_fk_parent_checks = .empty;
    }

    fn deferForeignKeyReferenceAbsenceCheck(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
    ) !void {
        const parent_key_owned = try self.alloc.dupe(u8, parent_key);
        var parent_key_transferred = false;
        errdefer if (!parent_key_transferred) self.alloc.free(parent_key_owned);
        try self.pending_fk_reference_absence_checks.append(self.alloc, .{
            .foreign_key = foreign_key,
            .parent_key = parent_key_owned,
        });
        parent_key_transferred = true;
    }

    fn validatePendingForeignKeyReferenceAbsenceChecks(self: *WriteParticipant) !void {
        for (self.pending_fk_reference_absence_checks.items) |check| {
            try self.requireNoRestrictingForeignKeyRefsForIdentity(check.foreign_key, check.parent_key);
        }
    }

    fn clearPendingForeignKeyReferenceAbsenceChecks(self: *WriteParticipant) void {
        for (self.pending_fk_reference_absence_checks.items) |check| {
            self.alloc.free(@constCast(check.parent_key));
        }
        self.pending_fk_reference_absence_checks.deinit(self.alloc);
        self.pending_fk_reference_absence_checks = .empty;
    }

    fn requireForeignKeyParentExists(self: *WriteParticipant, foreign_key: schema_mod.ForeignKey, parent_key: []const u8, ordered_parent_tuple: ?[]const u8, child_span: ?PeriodSpan) !void {
        if (foreign_key.child_period != null or foreign_key.parent_period != null) {
            const span = child_span orelse return error.ForeignKeyViolation;
            return try self.requireForeignKeyTemporalParentCovers(foreign_key, parent_key, span);
        }
        if (!foreignKeyReferencesPrimaryKey(foreign_key)) {
            const parent_constraint = self.findParentTupleConstraint(foreign_key.parent_columns) orelse return error.ForeignKeyViolation;
            return try self.requireForeignKeyUniqueParentExists(parent_constraint, parent_key, ordered_parent_tuple);
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

    fn requireForeignKeyTemporalParentCovers(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        child_span: PeriodSpan,
    ) !void {
        const parent_period = foreign_key.parent_period orelse return error.ForeignKeyViolation;
        const parent_constraint = self.findParentTupleConstraint(foreign_key.parent_columns) orelse return error.ForeignKeyViolation;
        if (parent_constraint.without_overlaps_period == null or
            !std.mem.eql(u8, parent_constraint.without_overlaps_period.?, parent_period))
        {
            return error.ForeignKeyViolation;
        }

        var covered_end = child_span.start;
        var matched_any = false;
        while (periodBoundLessThan(covered_end, child_span.end)) {
            const next = try self.findTemporalParentCoverageEnd(parent_constraint, parent_key, covered_end, child_span.end);
            if (next == null) return error.ForeignKeyViolation;
            if (!periodBoundLessThan(covered_end, next.?)) return error.ForeignKeyViolation;
            covered_end = next.?;
            matched_any = true;
        }
        if (!matched_any or !periodBoundEqual(covered_end, child_span.end)) return error.ForeignKeyViolation;
    }

    fn findTemporalParentCoverageEnd(
        self: *WriteParticipant,
        parent_constraint: schema_mod.UniqueConstraint,
        parent_key: []const u8,
        needed_start: PeriodBound,
        child_end: PeriodBound,
    ) !?PeriodBound {
        const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(self.alloc, parent_constraint.name, parent_key);
        defer self.alloc.free(prefix);

        var best: ?PeriodBound = null;
        for (self.writes.items) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            if (containsKey(self.planned_delete_keys, write.value)) continue;
            const span = try decodeTemporalUniqueSpanFromKeyAlloc(self.alloc, write.key, prefix);
            best = temporalCoverageCandidateEnd(best, needed_start, child_end, span);
        }

        const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(self.alloc, parent_constraint.name, parent_key);
        defer if (upper) |buf| self.alloc.free(buf);
        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            const owner = batchWriteValue(self.writes.items, entry.key) orelse entry.value;
            if (containsKey(self.planned_delete_keys, owner)) continue;
            const span = try decodeTemporalUniqueSpanFromKeyAlloc(self.alloc, entry.key, prefix);
            best = temporalCoverageCandidateEnd(best, needed_start, child_end, span);
        }
        return best;
    }

    fn findParentTupleConstraint(self: *const WriteParticipant, columns: []const []const u8) ?schema_mod.UniqueConstraint {
        if (self.primary_key) |primary_key| {
            if (stringSlicesEqual(primary_key.columns, columns)) return primaryKeyAsUniqueConstraint(primary_key);
        }
        return findUniqueConstraintByColumns(self.unique_constraints, columns);
    }

    fn orderedTupleParentValueForForeignKeyCellsAlloc(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        cells: []const relational_row_codec.Cell,
    ) !?[]u8 {
        if (foreignKeyReferencesPrimaryKey(foreign_key)) return null;
        const constraint = self.findParentTupleConstraint(foreign_key.parent_columns) orelse return null;
        const index = self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple) orelse return null;
        if (!orderedTupleUniqueIndexLookupReady(constraint, index)) return null;
        if (index.keys.len != constraint.columns.len) return null;

        const ordered_cells = try self.alloc.alloc(?relational_row_codec.Cell, index.keys.len);
        defer self.alloc.free(ordered_cells);
        const ordered_columns = try self.alloc.alloc(schema_mod.RelationalColumn, index.keys.len);
        defer self.alloc.free(ordered_columns);

        for (index.keys, 0..) |index_key, key_index| {
            const parent_column_index = foreignKeyParentColumnIndexForOrderedKey(self.relational_columns, foreign_key, index_key.column) orelse return null;
            const child_column = foreign_key.child_columns[parent_column_index];
            ordered_cells[key_index] = findCellInCells(cells, child_column) orelse return null;
            ordered_columns[key_index] = findRelationalColumn(self.relational_columns, index_key.column) orelse return null;
        }

        return try orderedTupleValueForMatchedIndexCellsAlloc(self.alloc, ordered_cells, ordered_columns, index.keys);
    }

    fn requireForeignKeyUniqueParentExists(self: *WriteParticipant, constraint: schema_mod.UniqueConstraint, encoded_value: []const u8, ordered_parent_tuple: ?[]const u8) !void {
        if (ordered_parent_tuple) |tuple| {
            if (self.column_index_policy.uniqueConstraintIndex(constraint, .ordered_tuple)) |index| {
                if (!orderedTupleUniqueIndexLookupReady(constraint, index)) return error.ForeignKeyViolation;
                if (try self.foreignKeyUniqueParentExistsViaOrderedTuple(index, tuple)) return;
                return error.ForeignKeyViolation;
            }
        }
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

    fn foreignKeyUniqueParentExistsViaOrderedTuple(
        self: *WriteParticipant,
        index: schema_mod.RelationalIndex,
        ordered_parent_tuple: []const u8,
    ) !bool {
        for (self.writes.items) |write| {
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (!std.mem.eql(u8, decoded.index_id, index.name)) continue;
            if (!std.mem.eql(u8, decoded.encoded_tuple, ordered_parent_tuple)) continue;
            if (try self.foreignKeyParentOwnerVisible(decoded.doc_key)) return true;
        }

        const owners = try scanOrderedTupleDocKeysAlloc(self.alloc, self.store, index.name, ordered_parent_tuple, "", "");
        defer freeDocKeys(self.alloc, owners);
        for (owners) |owner| {
            if (try self.foreignKeyParentOwnerVisible(owner)) return true;
        }
        return false;
    }

    fn foreignKeyParentOwnerVisible(self: *WriteParticipant, owner: []const u8) !bool {
        if (containsKey(self.planned_delete_keys, owner)) return false;
        const row_key = try rowKeyAlloc(self.alloc, owner);
        defer self.alloc.free(row_key);
        if (containsBatchDelete(self.deletes.items, row_key)) return false;
        if (containsBatchWrite(self.writes.items, row_key)) return true;
        const raw = try getRawAlloc(self.alloc, self.store, owner);
        if (raw) |value| {
            self.alloc.free(value);
            return true;
        }
        return false;
    }

    fn requireNoRestrictingPrimaryKeyForeignKeyRefs(self: *WriteParticipant, parent_key: []const u8) !void {
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!foreignKeyDeleteActionRestricts(foreign_key)) continue;
            if (!foreignKeyReferencesPrimaryKey(foreign_key)) continue;
            if (self.foreignKeyDefersNoActionDelete(foreign_key)) {
                try self.deferForeignKeyReferenceAbsenceCheck(foreign_key, parent_key);
            } else {
                try self.requireNoRestrictingForeignKeyRefsForIdentity(foreign_key, parent_key);
            }
        }
    }

    fn requireNoRestrictingUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
        parent_cells: []const relational_row_codec.Cell,
    ) !void {
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!foreignKeyDeleteActionRestricts(foreign_key)) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
            if (self.foreignKeyDefersNoActionDelete(foreign_key)) {
                try self.deferForeignKeyReferenceAbsenceCheck(foreign_key, encoded_value);
            } else {
                if (try self.requireNoRestrictingForeignKeyRefsViaChildOrderedTuple(foreign_key, encoded_value, parent_cells)) continue;
                try self.requireNoRestrictingForeignKeyRefsForIdentity(foreign_key, encoded_value);
            }
        }
    }

    fn requireNoUpdatingUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) !void {
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!foreignKeyUpdateActionRestricts(foreign_key)) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
            if (self.foreignKeyDefersNoActionUpdate(foreign_key)) {
                try self.deferForeignKeyReferenceAbsenceCheck(foreign_key, encoded_value);
            } else {
                try self.requireNoRestrictingForeignKeyRefsForIdentity(foreign_key, encoded_value);
            }
        }
    }

    fn applySetNullUpdatingUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        encoded_value: []const u8,
    ) anyerror!void {
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_update != .set_null) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
            try self.applySetNullForeignKeyRefsForIdentity(foreign_key, encoded_value);
        }
    }

    fn applyCascadeUpdatingUniqueForeignKeyRefs(
        self: *WriteParticipant,
        constraint: schema_mod.UniqueConstraint,
        old_encoded_value: []const u8,
        new_parent_row: []const u8,
    ) anyerror!void {
        if (!uniqueConstraintCanBackForeignKeyReferenceChecks(constraint)) return;
        for (self.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (foreign_key.on_update != .cascade) continue;
            if (!foreignKeyReferencesConstraintForRefCheck(foreign_key, constraint)) continue;
            try self.applyCascadeUpdateForeignKeyRefsForIdentity(foreign_key, old_encoded_value, new_parent_row);
        }
    }

    fn applyCascadeUpdateForeignKeyRefsForIdentity(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        old_parent_key: []const u8,
        new_parent_row: []const u8,
    ) anyerror!void {
        const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            old_parent_key,
        );
        defer self.alloc.free(prefix);
        const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(
            self.alloc,
            foreign_key.name,
            foreign_key.parent_table,
            old_parent_key,
        );
        defer if (upper) |buf| self.alloc.free(buf);

        const writes_end = self.writes.items.len;
        for (self.writes.items[0..writes_end]) |write| {
            if (!std.mem.startsWith(u8, write.key, prefix)) continue;
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareCascadeUpdateForeignKeyChild(foreign_key, old_parent_key, new_parent_row, decoded.child_key);
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            try self.prepareCascadeUpdateForeignKeyChild(foreign_key, old_parent_key, new_parent_row, decoded.child_key);
        }
    }

    fn prepareCascadeUpdateForeignKeyChild(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        old_parent_key: []const u8,
        new_parent_row: []const u8,
        child_key: []const u8,
    ) anyerror!void {
        if (containsKey(self.planned_delete_keys, child_key)) return;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return;
        defer self.alloc.free(row);

        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, old_parent_key)) return;

        const rewritten = try relationalRowWithForeignKeyColumnsFromParentAlloc(self.alloc, row, new_parent_row, foreign_key);
        var rewritten_owned = true;
        errdefer if (rewritten_owned) self.alloc.free(rewritten);
        try self.prepareUpsert(self.effectiveTableName(), child_key, rewritten, null);
        try self.owned_values.append(self.alloc, rewritten);
        rewritten_owned = false;
    }

    fn prepareCascadeUpdateForeignKeyChildFromParentKey(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        old_parent_key: []const u8,
        new_parent_key: []const u8,
        child_key: []const u8,
    ) anyerror!void {
        if (containsKey(self.planned_delete_keys, child_key)) return;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return;
        defer self.alloc.free(row);

        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, old_parent_key)) return;

        const rewritten = try relationalRowWithForeignKeyColumnsFromParentKeyAlloc(self.alloc, row, new_parent_key, foreign_key);
        var rewritten_owned = true;
        errdefer if (rewritten_owned) self.alloc.free(rewritten);
        try self.prepareUpsert(self.effectiveTableName(), child_key, rewritten, null);
        try self.owned_values.append(self.alloc, rewritten);
        rewritten_owned = false;
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
            if (try self.temporalForeignKeyChildRemainsCovered(foreign_key, parent_key, decoded.child_key)) continue;
            return error.ForeignKeyViolation;
        }

        const scanned = try self.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
        defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            if (containsBatchDelete(self.deletes.items, entry.key)) continue;
            var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (containsKey(self.planned_delete_keys, decoded.child_key)) continue;
            if (try self.temporalForeignKeyChildRemainsCovered(foreign_key, parent_key, decoded.child_key)) continue;
            return error.ForeignKeyViolation;
        }
    }

    fn requireNoRestrictingForeignKeyRefsViaChildOrderedTuple(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        parent_cells: []const relational_row_codec.Cell,
    ) !bool {
        const index = try self.readyChildOrderedTupleIndexForForeignKey(foreign_key, parent_cells) orelse return false;
        const tuple = try self.childOrderedTupleValueForForeignKeyParentCellsAlloc(foreign_key, parent_cells, index);
        defer self.alloc.free(tuple);
        const index_id = index.name;

        for (self.writes.items) |write| {
            if (containsBatchDelete(self.deletes.items, write.key)) continue;
            var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(self.alloc, write.key)) orelse continue;
            defer decoded.deinit(self.alloc);
            if (!std.mem.eql(u8, decoded.index_id, index_id)) continue;
            if (!std.mem.eql(u8, decoded.encoded_tuple, tuple)) continue;
            if (try self.restrictingForeignKeyCandidateVisible(foreign_key, parent_key, decoded.doc_key)) return error.ForeignKeyViolation;
        }

        const child_keys = try scanOrderedTupleDocKeysAlloc(self.alloc, self.store, index_id, tuple, "", "");
        defer freeDocKeys(self.alloc, child_keys);
        for (child_keys) |child_key| {
            if (try self.restrictingForeignKeyCandidateVisible(foreign_key, parent_key, child_key)) return error.ForeignKeyViolation;
        }
        return true;
    }

    fn restrictingForeignKeyCandidateVisible(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        child_key: []const u8,
    ) !bool {
        if (containsKey(self.planned_delete_keys, child_key)) return false;
        if (try self.temporalForeignKeyChildRemainsCovered(foreign_key, parent_key, child_key)) return false;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return false;
        defer self.alloc.free(row);
        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return false;
        defer self.alloc.free(current_parent);
        return std.mem.eql(u8, current_parent, parent_key);
    }

    fn readyChildOrderedTupleIndexForForeignKey(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_cells: []const relational_row_codec.Cell,
    ) !?schema_mod.RelationalIndex {
        if (foreign_key.child_period != null or foreign_key.parent_period != null) return null;
        var saw_covering_ordered_index = false;
        for (self.column_index_policy.indexes) |index| {
            if (index.owner_kind != .relational_column or index.access_method != .ordered_tuple) continue;
            if (self.column_index_policy.orderedTupleIndexOwnerColumn(index) == null) continue;
            if (!orderedTupleIndexCoversForeignKeyChildColumns(index, foreign_key)) continue;
            saw_covering_ordered_index = true;
            if (!orderedTupleIndexReadyForConstraintLookup(index)) return error.ForeignKeyViolation;
            if (index.where_expressions.len != 0) return error.ForeignKeyViolation;
            if (index.where.len != 0) {
                const child_cells = try self.foreignKeyChildCellsFromParentCellsAlloc(foreign_key, parent_cells);
                defer self.alloc.free(child_cells);
                if (!(try rowMatchesUniqueConstraintPredicatesFromCells(self.alloc, child_cells, index.where, self.relational_columns))) {
                    return error.ForeignKeyViolation;
                }
            }
            return index;
        }
        return if (saw_covering_ordered_index) error.ForeignKeyViolation else null;
    }

    fn foreignKeyChildCellsFromParentCellsAlloc(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_cells: []const relational_row_codec.Cell,
    ) ![]relational_row_codec.Cell {
        const child_cells = try self.alloc.alloc(relational_row_codec.Cell, foreign_key.child_columns.len);
        errdefer self.alloc.free(child_cells);
        for (foreign_key.child_columns, 0..) |child_column, index| {
            const parent_column = foreign_key.parent_columns[index];
            const parent_cell = findCellInCells(parent_cells, parent_column) orelse return error.ForeignKeyViolation;
            child_cells[index] = parent_cell;
            child_cells[index].path = child_column;
        }
        return child_cells;
    }

    fn childOrderedTupleValueForForeignKeyParentCellsAlloc(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_cells: []const relational_row_codec.Cell,
        index: schema_mod.RelationalIndex,
    ) ![]u8 {
        const ordered_cells = try self.alloc.alloc(?relational_row_codec.Cell, index.keys.len);
        defer self.alloc.free(ordered_cells);
        const ordered_columns = try self.alloc.alloc(schema_mod.RelationalColumn, index.keys.len);
        defer self.alloc.free(ordered_columns);

        for (index.keys, 0..) |index_key, key_index| {
            const child_column_index = foreignKeyChildColumnIndex(foreign_key, index_key.column) orelse return error.ForeignKeyViolation;
            const parent_column = foreign_key.parent_columns[child_column_index];
            ordered_cells[key_index] = findCellInCells(parent_cells, parent_column) orelse return error.ForeignKeyViolation;
            ordered_columns[key_index] = findRelationalColumn(self.relational_columns, index_key.column) orelse return error.ForeignKeyViolation;
        }

        return try orderedTupleValueForMatchedIndexCellsAlloc(self.alloc, ordered_cells, ordered_columns, index.keys);
    }

    fn temporalForeignKeyChildRemainsCovered(
        self: *WriteParticipant,
        foreign_key: schema_mod.ForeignKey,
        parent_key: []const u8,
        child_key: []const u8,
    ) !bool {
        if (foreign_key.child_period == null or foreign_key.parent_period == null) return false;
        const row = (try self.getPendingOrStoredRawRowAlloc(child_key)) orelse return false;
        defer self.alloc.free(row);

        const current_parent = (try self.foreignKeyReferenceValueAlloc(row, foreign_key)) orelse return false;
        defer self.alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, parent_key)) return true;

        const span = try self.periodSpanForRow(row, foreign_key.child_period.?);
        self.requireForeignKeyTemporalParentCovers(foreign_key, parent_key, span) catch |err| switch (err) {
            error.ForeignKeyViolation => return false,
            else => return err,
        };
        return true;
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

    fn foreignKeyReferenceValueAlloc(self: *const WriteParticipant, row_value: []const u8, foreign_key: schema_mod.ForeignKey) !?[]u8 {
        return try foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(self.alloc, row_value, foreign_key, self.primary_key, self.relational_columns);
    }

    fn foreignKeyReferenceValueForCellsAlloc(
        self: *const WriteParticipant,
        cells: []const relational_row_codec.Cell,
        foreign_key: schema_mod.ForeignKey,
    ) !?[]u8 {
        return try foreignKeyReferenceValueWithColumnsAndPrimaryKeyFromCellsAlloc(self.alloc, cells, foreign_key, self.primary_key, self.relational_columns);
    }
};

fn foreignKeyReferencesPrimaryKey(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.parent_columns.len == 1 and std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
}

fn foreignKeyIsEnforced(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.validation_state == .enforced;
}

fn foreignKeyDeleteActionRestricts(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.on_delete == .restrict or foreign_key.on_delete == .no_action;
}

fn foreignKeyUpdateActionRestricts(foreign_key: schema_mod.ForeignKey) bool {
    return foreign_key.on_update == .restrict or foreign_key.on_update == .no_action;
}

fn findUniqueConstraintByColumns(constraints: []const schema_mod.UniqueConstraint, columns: []const []const u8) ?schema_mod.UniqueConstraint {
    for (constraints) |constraint| {
        if (!uniqueConstraintCanBackForeignKey(constraint)) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn uniqueConstraintIsEnforced(constraint: schema_mod.UniqueConstraint) bool {
    return constraint.validation_state == .enforced;
}

fn uniqueConstraintCanBackForeignKey(constraint: schema_mod.UniqueConstraint) bool {
    return uniqueConstraintIsEnforced(constraint) and
        constraint.where.len == 0 and
        constraint.expressions.len == 0 and
        constraint.without_overlaps_period == null;
}

fn uniqueConstraintCanBackForeignKeyReferenceChecks(constraint: schema_mod.UniqueConstraint) bool {
    return uniqueConstraintIsEnforced(constraint) and
        constraint.where.len == 0 and
        constraint.expressions.len == 0;
}

fn foreignKeyReferencesConstraintForRefCheck(foreign_key: schema_mod.ForeignKey, constraint: schema_mod.UniqueConstraint) bool {
    if (!stringSlicesEqual(foreign_key.parent_columns, constraint.columns)) return false;
    if (constraint.without_overlaps_period) |period| {
        return foreign_key.parent_period != null and
            foreign_key.child_period != null and
            std.mem.eql(u8, foreign_key.parent_period.?, period);
    }
    return foreign_key.parent_period == null and foreign_key.child_period == null;
}

fn foreignKeyParentExists(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    relational_indexes: []const schema_mod.RelationalIndex,
    ordered_parent_tuple: ?[]const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
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

    const unique_constraint = if (primary_key) |key|
        if (stringSlicesEqual(key.columns, foreign_key.parent_columns)) primaryKeyAsUniqueConstraint(key) else findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return false
    else
        findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return false;
    if (relationalIndexForUniqueConstraint(relational_indexes, unique_constraint, .ordered_tuple)) |ordered_index| {
        if (!orderedTupleUniqueIndexLookupReady(unique_constraint, ordered_index)) return false;
        const tuple = ordered_parent_tuple orelse return false;
        const owners = try scanOrderedTupleDocKeysAlloc(alloc, store, ordered_index.name, tuple, "", "");
        defer freeDocKeys(alloc, owners);
        for (owners) |owner_doc_key| {
            const parent = try getRawAlloc(alloc, store, owner_doc_key);
            if (parent) |raw| {
                alloc.free(raw);
                return true;
            }
        }
        return false;
    }
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

fn foreignKeyParentExistsForChildRow(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    relational_indexes: []const schema_mod.RelationalIndex,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    parent_key: []const u8,
) !bool {
    if (foreign_key.child_period == null and foreign_key.parent_period == null) {
        const ordered_parent_tuple = try orderedTupleParentValueForForeignKeyRowAlloc(alloc, row_value, foreign_key, primary_key, unique_constraints, relational_indexes, columns);
        defer if (ordered_parent_tuple) |tuple| alloc.free(tuple);
        return try foreignKeyParentExists(alloc, store, relational_indexes, ordered_parent_tuple, foreign_key, primary_key, unique_constraints, parent_key);
    }
    const child_period = foreign_key.child_period orelse return false;
    const parent_period = foreign_key.parent_period orelse return false;
    const child_span = try periodSpanForRowWithCatalog(alloc, columns, periods, row_value, child_period);
    const parent_constraint = if (primary_key) |key|
        if (stringSlicesEqual(key.columns, foreign_key.parent_columns)) primaryKeyAsUniqueConstraint(key) else findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return false
    else
        findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return false;
    if (parent_constraint.without_overlaps_period == null or
        !std.mem.eql(u8, parent_constraint.without_overlaps_period.?, parent_period))
    {
        return false;
    }
    return try temporalForeignKeyParentCovers(alloc, store, parent_constraint, parent_key, child_span);
}

pub fn orderedTupleParentValueForForeignKeyRowAlloc(
    alloc: Allocator,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return null;
    const parent_constraint = if (primary_key) |key|
        if (stringSlicesEqual(key.columns, foreign_key.parent_columns)) primaryKeyAsUniqueConstraint(key) else findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return null
    else
        findUniqueConstraintByColumns(unique_constraints, foreign_key.parent_columns) orelse return null;
    const index = relationalIndexForUniqueConstraint(relational_indexes, parent_constraint, .ordered_tuple) orelse return null;
    if (!orderedTupleUniqueIndexLookupReady(parent_constraint, index)) return null;
    if (index.keys.len != parent_constraint.columns.len) return null;

    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    const ordered_cells = try alloc.alloc(?relational_row_codec.Cell, index.keys.len);
    defer alloc.free(ordered_cells);
    const ordered_columns = try alloc.alloc(schema_mod.RelationalColumn, index.keys.len);
    defer alloc.free(ordered_columns);

    for (index.keys, 0..) |index_key, key_index| {
        const parent_column_index = foreignKeyParentColumnIndexForOrderedKey(columns, foreign_key, index_key.column) orelse return null;
        const child_column = foreign_key.child_columns[parent_column_index];
        ordered_cells[key_index] = findCellInCells(row.cells, child_column) orelse return null;
        ordered_columns[key_index] = findRelationalColumn(columns, index_key.column) orelse return null;
    }

    return try orderedTupleValueForMatchedIndexCellsAlloc(alloc, ordered_cells, ordered_columns, index.keys);
}

pub fn readyChildOrderedTupleIndexForForeignKey(
    relational_indexes: []const schema_mod.RelationalIndex,
    foreign_key: schema_mod.ForeignKey,
) !?schema_mod.RelationalIndex {
    if (foreign_key.child_period != null or foreign_key.parent_period != null) return null;
    var saw_covering_ordered_index = false;
    for (relational_indexes) |index| {
        if (index.owner_kind != .relational_column or index.access_method != .ordered_tuple) continue;
        if (!orderedTupleIndexCoversForeignKeyChildColumns(index, foreign_key)) continue;
        saw_covering_ordered_index = true;
        if (!orderedTupleIndexReadyForConstraintLookup(index)) return error.ForeignKeyViolation;
        if (index.where.len != 0 or index.where_expressions.len != 0) return error.ForeignKeyViolation;
        return index;
    }
    return if (saw_covering_ordered_index) error.ForeignKeyViolation else null;
}

pub fn orderedTupleChildValueForForeignKeyParentRowAlloc(
    alloc: Allocator,
    parent_row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    child_relational_indexes: []const schema_mod.RelationalIndex,
    child_columns: []const schema_mod.RelationalColumn,
    parent_columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    const index = try readyChildOrderedTupleIndexForForeignKey(child_relational_indexes, foreign_key) orelse return null;
    var parent_row = try relational_row_codec.deserialize(alloc, parent_row_value);
    defer parent_row.deinit(alloc);

    const ordered_cells = try alloc.alloc(?relational_row_codec.Cell, index.keys.len);
    defer alloc.free(ordered_cells);
    const ordered_columns = try alloc.alloc(schema_mod.RelationalColumn, index.keys.len);
    defer alloc.free(ordered_columns);

    for (index.keys, 0..) |index_key, key_index| {
        const child_column_index = foreignKeyChildColumnIndex(foreign_key, index_key.column) orelse return error.ForeignKeyViolation;
        const parent_column = foreign_key.parent_columns[child_column_index];
        ordered_cells[key_index] = findCellInCells(parent_row.cells, parent_column) orelse return error.ForeignKeyViolation;
        if (findRelationalColumn(parent_columns, parent_column) == null) return error.ForeignKeyViolation;
        ordered_columns[key_index] = findRelationalColumn(child_columns, index_key.column) orelse return error.ForeignKeyViolation;
    }

    return try orderedTupleValueForMatchedIndexCellsAlloc(alloc, ordered_cells, ordered_columns, index.keys);
}

fn temporalForeignKeyParentCovers(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    parent_constraint: schema_mod.UniqueConstraint,
    parent_key: []const u8,
    child_span: PeriodSpan,
) !bool {
    var covered_end = child_span.start;
    var matched_any = false;
    while (periodBoundLessThan(covered_end, child_span.end)) {
        const next = try temporalForeignKeyParentCoverageEnd(alloc, store, parent_constraint, parent_key, covered_end, child_span.end);
        if (next == null) return false;
        if (!periodBoundLessThan(covered_end, next.?)) return false;
        covered_end = next.?;
        matched_any = true;
    }
    return matched_any and periodBoundEqual(covered_end, child_span.end);
}

fn temporalForeignKeyParentCoverageEnd(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    parent_constraint: schema_mod.UniqueConstraint,
    parent_key: []const u8,
    needed_start: PeriodBound,
    child_end: PeriodBound,
) !?PeriodBound {
    const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(alloc, parent_constraint.name, parent_key);
    defer alloc.free(prefix);
    const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, parent_constraint.name, parent_key);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var best: ?PeriodBound = null;
    for (scanned) |entry| {
        const parent_row = try getRawAlloc(alloc, store, entry.value);
        if (parent_row) |raw| {
            alloc.free(raw);
        } else {
            continue;
        }
        const span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, entry.key, prefix);
        best = temporalCoverageCandidateEnd(best, needed_start, child_end, span);
    }
    return best;
}

pub fn foreignKeyReferenceValueAlloc(alloc: Allocator, row_value: []const u8, foreign_key: schema_mod.ForeignKey) !?[]u8 {
    return try foreignKeyReferenceValueWithPrimaryKeyAlloc(alloc, row_value, foreign_key, null);
}

pub fn foreignKeyReferenceValueWithColumnsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    return try foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(alloc, row_value, foreign_key, primary_key, columns);
}

fn foreignKeyReferenceValueWithPrimaryKeyAlloc(
    alloc: Allocator,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
) !?[]u8 {
    return try foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(alloc, row_value, foreign_key, primary_key, &.{});
}

fn foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(
    alloc: Allocator,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (!foreignKeyReferencesPrimaryKey(foreign_key)) {
        const parent_is_primary_key = if (primary_key) |key| stringSlicesEqual(key.columns, foreign_key.parent_columns) else false;
        return try foreignKeyCompositeReferenceValueAlloc(alloc, row_value, foreign_key, parent_is_primary_key, columns);
    }
    return try foreignKeyPrimaryKeyValueAlloc(alloc, row_value, foreign_key.child_columns[0]);
}

fn foreignKeyReferenceValueWithColumnsAndPrimaryKeyFromCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    foreign_key: schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (!foreignKeyReferencesPrimaryKey(foreign_key)) {
        const parent_is_primary_key = if (primary_key) |key| stringSlicesEqual(key.columns, foreign_key.parent_columns) else false;
        return try foreignKeyCompositeReferenceValueFromCellsAlloc(alloc, cells, foreign_key, parent_is_primary_key, columns);
    }
    return try foreignKeyPrimaryKeyValueFromCellsAlloc(alloc, cells, foreign_key.child_columns[0]);
}

fn foreignKeyCompositeReferenceValueAlloc(
    alloc: Allocator,
    row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
    allow_partial_primary_key_reference_absence: bool,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var present_components: usize = 0;
    for (foreign_key.child_columns, 0..) |column_path, index| {
        const parent_column = if (!allow_partial_primary_key_reference_absence and index < foreign_key.parent_columns.len) findRelationalColumn(columns, foreign_key.parent_columns[index]) else null;
        const collation = if (parent_column) |column| column.collation else null;
        const component = (try uniqueConstraintColumnValueWithCollationAlloc(alloc, row_value, column_path, collation)) orelse continue;
        defer alloc.free(component);
        present_components += 1;
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }

    if (present_components == 0) {
        out.deinit(alloc);
        return null;
    }
    if (present_components != foreign_key.child_columns.len) {
        if (foreign_key.match == .simple or allow_partial_primary_key_reference_absence) {
            out.deinit(alloc);
            return null;
        }
        return error.ForeignKeyViolation;
    }
    return try out.toOwnedSlice(alloc);
}

fn foreignKeyCompositeReferenceValueFromCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    foreign_key: schema_mod.ForeignKey,
    allow_partial_primary_key_reference_absence: bool,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var present_components: usize = 0;
    for (foreign_key.child_columns, 0..) |column_path, index| {
        const parent_column = if (!allow_partial_primary_key_reference_absence and index < foreign_key.parent_columns.len) findRelationalColumn(columns, foreign_key.parent_columns[index]) else null;
        const collation = if (parent_column) |column| column.collation else null;
        const component = (try uniqueConstraintColumnValueFromCellsWithCollationAlloc(alloc, cells, column_path, collation)) orelse continue;
        defer alloc.free(component);
        present_components += 1;
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }

    if (present_components == 0) {
        out.deinit(alloc);
        return null;
    }
    if (present_components != foreign_key.child_columns.len) {
        if (foreign_key.match == .simple or allow_partial_primary_key_reference_absence) {
            out.deinit(alloc);
            return null;
        }
        return error.ForeignKeyViolation;
    }
    return try out.toOwnedSlice(alloc);
}

fn foreignKeyPrimaryKeyValueAlloc(alloc: Allocator, row_value: []const u8, column_path: []const u8) !?[]u8 {
    const cell = (try relational_row_codec.findCellByPath(row_value, column_path)) orelse return null;
    if (cell.value_type != .bytes_val) return error.InvalidColumnValue;
    if (cell.value.bytes_val.len == 0) return null;
    return try alloc.dupe(u8, cell.value.bytes_val);
}

fn foreignKeyPrimaryKeyValueFromCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    column_path: []const u8,
) !?[]u8 {
    const cell = findCellInRow(cells, column_path) orelse return null;
    if (cell.value_type != .bytes_val) return error.InvalidColumnValue;
    if (cell.value.bytes_val.len == 0) return null;
    return try alloc.dupe(u8, cell.value.bytes_val);
}

pub fn relationalRowWithoutColumnsAlloc(alloc: Allocator, row_value: []const u8, columns: []const []const u8) ![]u8 {
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

fn relationalRowWithForeignKeyColumnsFromParentAlloc(
    alloc: Allocator,
    child_row_value: []const u8,
    parent_row_value: []const u8,
    foreign_key: schema_mod.ForeignKey,
) ![]u8 {
    var child_row = try relational_row_codec.deserialize(alloc, child_row_value);
    defer child_row.deinit(alloc);
    var parent_row = try relational_row_codec.deserialize(alloc, parent_row_value);
    defer parent_row.deinit(alloc);

    var cells = std.ArrayListUnmanaged(relational_row_codec.Cell).empty;
    defer cells.deinit(alloc);
    var owned_values = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_values.items) |value| alloc.free(value);
        owned_values.deinit(alloc);
    }
    var replaced = try alloc.alloc(bool, foreign_key.child_columns.len);
    defer alloc.free(replaced);
    @memset(replaced, false);

    for (child_row.cells) |cell| {
        const replacement_index = foreignKeyChildColumnIndex(foreign_key, cell.path) orelse {
            try cells.append(alloc, cell);
            continue;
        };
        const parent_cell = findCellInRow(parent_row.cells, foreign_key.parent_columns[replacement_index]) orelse return error.InvalidColumnValue;
        const cloned_value = try cloneTypedValue(alloc, parent_cell.value_type, parent_cell.value);
        var value_owned = parent_cell.value_type == .bytes_val;
        errdefer if (value_owned) alloc.free(cloned_value.bytes_val);
        try cells.append(alloc, .{
            .path = foreign_key.child_columns[replacement_index],
            .value_type = parent_cell.value_type,
            .is_json = parent_cell.is_json,
            .value = cloned_value,
        });
        if (value_owned) {
            try owned_values.append(alloc, @constCast(cloned_value.bytes_val));
            value_owned = false;
        }
        replaced[replacement_index] = true;
    }

    for (replaced, 0..) |was_replaced, i| {
        if (was_replaced) continue;
        const parent_cell = findCellInRow(parent_row.cells, foreign_key.parent_columns[i]) orelse return error.InvalidColumnValue;
        const cloned_value = try cloneTypedValue(alloc, parent_cell.value_type, parent_cell.value);
        var value_owned = parent_cell.value_type == .bytes_val;
        errdefer if (value_owned) alloc.free(cloned_value.bytes_val);
        try cells.append(alloc, .{
            .path = foreign_key.child_columns[i],
            .value_type = parent_cell.value_type,
            .is_json = parent_cell.is_json,
            .value = cloned_value,
        });
        if (value_owned) {
            try owned_values.append(alloc, @constCast(cloned_value.bytes_val));
            value_owned = false;
        }
    }

    return try relational_row_codec.serialize(alloc, cells.items);
}

pub fn relationalRowWithForeignKeyColumnsFromParentKeyAlloc(
    alloc: Allocator,
    child_row_value: []const u8,
    encoded_parent_key: []const u8,
    foreign_key: schema_mod.ForeignKey,
) ![]u8 {
    var child_row = try relational_row_codec.deserialize(alloc, child_row_value);
    defer child_row.deinit(alloc);

    const replacement_cells = try foreignKeyParentKeyReplacementCellsAlloc(alloc, encoded_parent_key, foreign_key);
    defer {
        for (replacement_cells) |cell| {
            if (cell.value_type == .bytes_val) alloc.free(cell.value.bytes_val);
        }
        alloc.free(replacement_cells);
    }

    var cells = std.ArrayListUnmanaged(relational_row_codec.Cell).empty;
    defer cells.deinit(alloc);
    var replaced = try alloc.alloc(bool, replacement_cells.len);
    defer alloc.free(replaced);
    @memset(replaced, false);

    for (child_row.cells) |cell| {
        const replacement_index = foreignKeyChildColumnIndex(foreign_key, cell.path) orelse {
            try cells.append(alloc, cell);
            continue;
        };
        try cells.append(alloc, replacement_cells[replacement_index]);
        replaced[replacement_index] = true;
    }

    for (replaced, 0..) |was_replaced, i| {
        if (!was_replaced) try cells.append(alloc, replacement_cells[i]);
    }

    return try relational_row_codec.serialize(alloc, cells.items);
}

fn foreignKeyParentKeyReplacementCellsAlloc(
    alloc: Allocator,
    encoded_parent_key: []const u8,
    foreign_key: schema_mod.ForeignKey,
) ![]relational_row_codec.Cell {
    var cells = try alloc.alloc(relational_row_codec.Cell, foreign_key.child_columns.len);
    var initialized: usize = 0;
    errdefer {
        for (cells[0..initialized]) |cell| {
            if (cell.value_type == .bytes_val) alloc.free(cell.value.bytes_val);
        }
        alloc.free(cells);
    }

    if (foreignKeyReferencesPrimaryKey(foreign_key)) {
        if (cells.len != 1) return error.InvalidColumnValue;
        cells[0] = .{
            .path = foreign_key.child_columns[0],
            .value_type = .bytes_val,
            .is_json = false,
            .value = .{ .bytes_val = try alloc.dupe(u8, encoded_parent_key) },
        };
        initialized = 1;
        return cells;
    }

    var pos: usize = 0;
    for (foreign_key.child_columns, 0..) |child_column, i| {
        const term = internal_keys.findComponentTerminator(encoded_parent_key, pos) orelse return error.InvalidColumnValue;
        cells[i] = try foreignKeyParentKeyComponentCellAlloc(alloc, child_column, encoded_parent_key[pos..term]);
        initialized += 1;
        pos = term + 2;
    }
    if (pos != encoded_parent_key.len) return error.InvalidColumnValue;
    return cells;
}

fn foreignKeyParentKeyComponentCellAlloc(
    alloc: Allocator,
    child_column: []const u8,
    encoded_component: []const u8,
) !relational_row_codec.Cell {
    const component = try internal_keys.decodeBodyAlloc(alloc, encoded_component);
    defer alloc.free(component);
    if (component.len == 0) return error.InvalidColumnValue;
    const value_type = typedValueTypeFromByte(component[0]) orelse return error.InvalidColumnValue;
    const payload = component[1..];
    return switch (value_type) {
        .u64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            break :blk .{
                .path = child_column,
                .value_type = .u64_val,
                .is_json = false,
                .value = .{ .u64_val = std.mem.readInt(u64, payload[0..8], .big) },
            };
        },
        .i64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            const sortable_bits = std.mem.readInt(u64, payload[0..8], .big);
            break :blk .{
                .path = child_column,
                .value_type = .i64_val,
                .is_json = false,
                .value = .{ .i64_val = @bitCast(sortable_bits ^ 0x8000_0000_0000_0000) },
            };
        },
        .f64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            break :blk .{
                .path = child_column,
                .value_type = .f64_val,
                .is_json = false,
                .value = .{ .f64_val = @bitCast(std.mem.readInt(u64, payload[0..8], .big)) },
            };
        },
        .bool_val => blk: {
            if (payload.len != 1) return error.InvalidColumnValue;
            break :blk .{
                .path = child_column,
                .value_type = .bool_val,
                .is_json = false,
                .value = .{ .bool_val = payload[0] != 0 },
            };
        },
        .geo_point => blk: {
            if (payload.len != 16) return error.InvalidColumnValue;
            break :blk .{
                .path = child_column,
                .value_type = .geo_point,
                .is_json = false,
                .value = .{ .geo_point = .{
                    .lat = @bitCast(std.mem.readInt(u64, payload[0..8], .big)),
                    .lon = @bitCast(std.mem.readInt(u64, payload[8..16], .big)),
                } },
            };
        },
        .bytes_val => .{
            .path = child_column,
            .value_type = .bytes_val,
            .is_json = false,
            .value = .{ .bytes_val = try alloc.dupe(u8, payload) },
        },
    };
}

fn foreignKeyChildColumnIndex(foreign_key: schema_mod.ForeignKey, child_column: []const u8) ?usize {
    for (foreign_key.child_columns, 0..) |column, i| {
        if (std.mem.eql(u8, column, child_column)) return i;
    }
    return null;
}

fn findCellInRow(cells: []const relational_row_codec.Cell, path: []const u8) ?relational_row_codec.Cell {
    for (cells) |cell| {
        if (std.mem.eql(u8, cell.path, path)) return cell;
    }
    return null;
}

fn findCellByColumnPathInCells(
    cells: []const relational_row_codec.Cell,
    column_path: []const u8,
    columns: []const schema_mod.RelationalColumn,
) ?relational_row_codec.Cell {
    if (findRelationalColumn(columns, column_path)) |column| {
        if (findCellInRow(cells, column.path)) |cell| return cell;
        if (findCellInRow(cells, column.name)) |cell| return cell;
    }
    return findCellInRow(cells, column_path);
}

fn periodStartBeforeEnd(
    columns: []const schema_mod.RelationalColumn,
    cells: []const relational_row_codec.Cell,
    period: schema_mod.RelationalPeriod,
) bool {
    const span = periodSpanFromCells(columns, cells, period) catch return false;
    return periodBoundLessThan(span.start, span.end);
}

fn periodSpanFromCells(
    columns: []const schema_mod.RelationalColumn,
    cells: []const relational_row_codec.Cell,
    period: schema_mod.RelationalPeriod,
) !PeriodSpan {
    const start_column = findRelationalColumn(columns, period.start_column) orelse return error.InvalidColumnValue;
    const end_column = findRelationalColumn(columns, period.end_column) orelse return error.InvalidColumnValue;
    if (start_column.field_type != end_column.field_type) return error.InvalidColumnValue;
    const start = try periodStartBoundFromCell(findCellInRow(cells, period.start_column), start_column);
    const end = try periodEndBoundFromCell(findCellInRow(cells, period.end_column), end_column);
    const span: PeriodSpan = .{ .start = start, .end = end };
    if (!periodBoundLessThan(span.start, span.end)) return error.InvalidColumnValue;
    return span;
}

fn periodStartBoundFromCell(cell: ?relational_row_codec.Cell, column: schema_mod.RelationalColumn) !PeriodBound {
    return if (cell) |present| try periodBoundFromCell(present, column) else if (column.nullable) .neg_infinity else error.InvalidColumnValue;
}

fn periodEndBoundFromCell(cell: ?relational_row_codec.Cell, column: schema_mod.RelationalColumn) !PeriodBound {
    return if (cell) |present| try periodBoundFromCell(present, column) else if (column.nullable) .pos_infinity else error.InvalidColumnValue;
}

fn periodBoundFromCell(cell: relational_row_codec.Cell, column: schema_mod.RelationalColumn) !PeriodBound {
    return switch (column.field_type) {
        .numeric => if (cell.value_type == .f64_val) .{ .f64_val = cell.value.f64_val } else error.InvalidColumnValue,
        .datetime => if (cell.value_type == .u64_val) .{ .i64_val = @as(i64, @bitCast(cell.value.u64_val)) } else error.InvalidColumnValue,
        else => error.InvalidColumnValue,
    };
}

fn findRelationalColumn(columns: []const schema_mod.RelationalColumn, name: []const u8) ?schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name) or std.mem.eql(u8, column.path, name)) return column;
    }
    return null;
}

fn periodBoundLessThan(left: PeriodBound, right: PeriodBound) bool {
    return switch (left) {
        .neg_infinity => right != .neg_infinity,
        .f64_val => |value| switch (right) {
            .neg_infinity => false,
            .f64_val => |other| value < other,
            .pos_infinity => true,
            else => false,
        },
        .i64_val => |value| switch (right) {
            .neg_infinity => false,
            .i64_val => |other| value < other,
            .pos_infinity => true,
            else => false,
        },
        .pos_infinity => false,
    };
}

fn periodBoundsEqual(left: PeriodBound, right: PeriodBound) bool {
    return switch (left) {
        .neg_infinity => right == .neg_infinity,
        .f64_val => |value| switch (right) {
            .f64_val => |other| value == other,
            else => false,
        },
        .i64_val => |value| switch (right) {
            .i64_val => |other| value == other,
            else => false,
        },
        .pos_infinity => right == .pos_infinity,
    };
}

fn periodSpansOverlap(left: PeriodSpan, right: PeriodSpan) bool {
    return periodBoundLessThan(left.start, right.end) and periodBoundLessThan(right.start, left.end);
}

fn periodSpanContainsPoint(span: PeriodSpan, point: PeriodBound) bool {
    return !periodBoundLessThan(point, span.start) and periodBoundLessThan(point, span.end);
}

fn temporalCoverageCandidateEnd(current_best: ?PeriodBound, needed_start: PeriodBound, child_end: PeriodBound, parent_span: PeriodSpan) ?PeriodBound {
    if (periodBoundLessThan(needed_start, parent_span.start)) return current_best;
    if (!periodBoundLessThan(needed_start, parent_span.end)) return current_best;
    const candidate = if (periodBoundLessThan(child_end, parent_span.end)) child_end else parent_span.end;
    if (current_best) |best| {
        return if (periodBoundLessThan(best, candidate)) candidate else best;
    }
    return candidate;
}

fn optionalPeriodSpanEqual(left: ?PeriodSpan, right: ?PeriodSpan) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return periodBoundEqual(left.?.start, right.?.start) and periodBoundEqual(left.?.end, right.?.end);
}

fn externalizedTemporalParentCheckMatchesSpan(
    alloc: Allocator,
    check: ExternalizedForeignKeyParentCheck,
    span: PeriodSpan,
) bool {
    const start_json = check.child_period_start_json orelse return false;
    const end_json = check.child_period_end_json orelse return false;
    const start = periodBoundFromJsonAlloc(alloc, start_json, span.start) catch return false;
    const end = periodBoundFromJsonAlloc(alloc, end_json, span.end) catch return false;
    return periodBoundEqual(start, span.start) and periodBoundEqual(end, span.end);
}

fn periodBoundFromJsonAlloc(alloc: Allocator, json: []const u8, expected: PeriodBound) !PeriodBound {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, json, .{}) catch return error.InvalidColumnValue;
    defer parsed.deinit();
    return switch (expected) {
        .neg_infinity => switch (parsed.value) {
            .null => .neg_infinity,
            else => error.InvalidColumnValue,
        },
        .f64_val => switch (parsed.value) {
            .integer => |value| .{ .f64_val = @floatFromInt(value) },
            .float => |value| .{ .f64_val = value },
            else => error.InvalidColumnValue,
        },
        .i64_val => switch (parsed.value) {
            .integer => |value| .{ .i64_val = value },
            else => error.InvalidColumnValue,
        },
        .pos_infinity => switch (parsed.value) {
            .null => .pos_infinity,
            else => error.InvalidColumnValue,
        },
    };
}

fn periodBoundEqual(left: PeriodBound, right: PeriodBound) bool {
    return switch (left) {
        .neg_infinity => right == .neg_infinity,
        .f64_val => |value| switch (right) {
            .f64_val => |other| value == other,
            else => false,
        },
        .i64_val => |value| switch (right) {
            .i64_val => |other| value == other,
            else => false,
        },
        .pos_infinity => right == .pos_infinity,
    };
}

fn temporalUniqueConstraintKeyAlloc(
    alloc: Allocator,
    constraint: schema_mod.UniqueConstraint,
    encoded_value: []const u8,
    span: PeriodSpan,
    doc_key: []const u8,
) ![]u8 {
    const start = try periodBoundBytesAlloc(alloc, span.start);
    defer alloc.free(start);
    const end = try periodBoundBytesAlloc(alloc, span.end);
    defer alloc.free(end);
    return try internal_keys.relationalTemporalUniqueKeyAlloc(alloc, constraint.name, encoded_value, start, end, doc_key);
}

fn periodBoundBytesAlloc(alloc: Allocator, bound: PeriodBound) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    switch (bound) {
        .neg_infinity => try out.append(alloc, temporal_bound_neg_infinity_tag),
        .f64_val => |value| {
            try out.append(alloc, @intFromEnum(typed_dv.ValueType.f64_val));
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, @bitCast(value), .big);
            try out.appendSlice(alloc, &buf);
        },
        .i64_val => |value| {
            try out.append(alloc, @intFromEnum(typed_dv.ValueType.u64_val));
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, @bitCast(value), .big);
            try out.appendSlice(alloc, &buf);
        },
        .pos_infinity => try out.append(alloc, temporal_bound_pos_infinity_tag),
    }
    return try out.toOwnedSlice(alloc);
}

fn decodeTemporalUniqueSpanFromKeyAlloc(alloc: Allocator, key: []const u8, prefix: []const u8) !PeriodSpan {
    if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidColumnValue;
    var pos: usize = prefix.len;
    const start_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidColumnValue;
    const start = try internal_keys.decodeBodyAlloc(alloc, key[pos..start_term]);
    defer alloc.free(start);
    pos = start_term + 2;
    const end_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidColumnValue;
    const end = try internal_keys.decodeBodyAlloc(alloc, key[pos..end_term]);
    defer alloc.free(end);
    return .{ .start = try periodBoundFromBytes(start), .end = try periodBoundFromBytes(end) };
}

fn periodBoundFromBytes(bytes: []const u8) !PeriodBound {
    if (bytes.len == 1 and bytes[0] == temporal_bound_neg_infinity_tag) return .neg_infinity;
    if (bytes.len == 1 and bytes[0] == temporal_bound_pos_infinity_tag) return .pos_infinity;
    if (bytes.len != 9) return error.InvalidColumnValue;
    const raw = std.mem.readInt(u64, bytes[1..9], .big);
    if (bytes[0] == @intFromEnum(typed_dv.ValueType.f64_val)) return .{ .f64_val = @bitCast(raw) };
    if (bytes[0] == @intFromEnum(typed_dv.ValueType.u64_val)) return .{ .i64_val = @bitCast(raw) };
    return error.InvalidColumnValue;
}

fn periodSpanForRowWithCatalog(
    alloc: Allocator,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    row_value: []const u8,
    period_name: []const u8,
) !PeriodSpan {
    const period = findPeriodByName(periods, period_name) orelse return error.InvalidColumnValue;
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    return try periodSpanFromCells(columns, row.cells, period);
}

fn findPeriodByName(periods: []const schema_mod.RelationalPeriod, name: []const u8) ?schema_mod.RelationalPeriod {
    for (periods) |period| {
        if (std.mem.eql(u8, period.name, name)) return period;
    }
    return null;
}

fn requireTemporalUniqueAvailableInBatchAndStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: []const docstore_mod.KVPair,
    deletes: []const []const u8,
    planned_delete_keys: []const []const u8,
    constraint: schema_mod.UniqueConstraint,
    encoded_value: []const u8,
    span: PeriodSpan,
    doc_key: []const u8,
) !void {
    const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(alloc, constraint.name, encoded_value);
    defer alloc.free(prefix);

    for (writes) |write| {
        if (!std.mem.startsWith(u8, write.key, prefix)) continue;
        if (containsBatchDelete(deletes, write.key)) continue;
        if (std.mem.eql(u8, write.value, doc_key)) continue;
        const existing_span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, write.key, prefix);
        if (periodSpansOverlap(span, existing_span)) return error.UniqueConstraintViolation;
    }

    const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, constraint.name, encoded_value);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);
    for (scanned) |entry| {
        if (containsBatchDelete(deletes, entry.key)) continue;
        const owner = batchWriteValue(writes, entry.key) orelse entry.value;
        if (std.mem.eql(u8, owner, doc_key)) continue;
        if (containsKey(planned_delete_keys, owner)) continue;
        const existing_span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, entry.key, prefix);
        if (periodSpansOverlap(span, existing_span)) return error.UniqueConstraintViolation;
    }
}

pub fn lookupTemporalUniqueOwnerAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_point: []const u8,
) !?[]u8 {
    const point = try periodBoundFromBytes(encoded_point);
    const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(prefix);
    const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, constraint_name, encoded_value);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);
    var owner: ?[]u8 = null;
    errdefer if (owner) |value| alloc.free(value);
    for (scanned) |entry| {
        const span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, entry.key, prefix);
        if (!periodSpanContainsPoint(span, point)) continue;
        if (owner) |existing| {
            if (!std.mem.eql(u8, existing, entry.value)) return error.UniqueConstraintViolation;
            continue;
        }
        owner = try alloc.dupe(u8, entry.value);
    }
    return owner;
}

pub fn lookupTemporalUniqueOverlapOwnerAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
) !?[]u8 {
    const query: PeriodSpan = .{
        .start = try periodBoundFromBytes(encoded_start),
        .end = try periodBoundFromBytes(encoded_end),
    };
    if (!periodBoundLessThan(query.start, query.end)) return error.InvalidColumnValue;
    const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(alloc, constraint_name, encoded_value);
    defer alloc.free(prefix);
    const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(alloc, constraint_name, encoded_value);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);
    var owner: ?[]u8 = null;
    errdefer if (owner) |value| alloc.free(value);
    for (scanned) |entry| {
        const span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, entry.key, prefix);
        if (!periodSpansOverlap(query, span)) continue;
        if (owner) |existing| {
            if (!std.mem.eql(u8, existing, entry.value)) return error.UniqueConstraintViolation;
            continue;
        }
        owner = try alloc.dupe(u8, entry.value);
    }
    return owner;
}

pub fn temporalUniqueKeyContainsPointAlloc(
    alloc: Allocator,
    key: []const u8,
    prefix: []const u8,
    encoded_point: []const u8,
) !bool {
    const point = try periodBoundFromBytes(encoded_point);
    const span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, key, prefix);
    return periodSpanContainsPoint(span, point);
}

pub fn temporalUniqueKeyOverlapsSpanAlloc(
    alloc: Allocator,
    key: []const u8,
    prefix: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
) !bool {
    const query: PeriodSpan = .{
        .start = try periodBoundFromBytes(encoded_start),
        .end = try periodBoundFromBytes(encoded_end),
    };
    if (!periodBoundLessThan(query.start, query.end)) return error.InvalidColumnValue;
    const span = try decodeTemporalUniqueSpanFromKeyAlloc(alloc, key, prefix);
    return periodSpansOverlap(query, span);
}

pub fn temporalPeriodSpanBytesValid(encoded_start: []const u8, encoded_end: []const u8) !bool {
    const query: PeriodSpan = .{
        .start = try periodBoundFromBytes(encoded_start),
        .end = try periodBoundFromBytes(encoded_end),
    };
    return periodBoundLessThan(query.start, query.end);
}

pub fn temporalPeriodBoundBytesOrder(left_bytes: []const u8, right_bytes: []const u8) !std.math.Order {
    const left = try periodBoundFromBytes(left_bytes);
    const right = try periodBoundFromBytes(right_bytes);
    if (periodBoundsEqual(left, right)) return .eq;
    return if (periodBoundLessThan(left, right)) .lt else .gt;
}

pub fn temporalPeriodSpanBytesOverlap(
    left_start: []const u8,
    left_end: []const u8,
    right_start: []const u8,
    right_end: []const u8,
) !bool {
    const left: PeriodSpan = .{
        .start = try periodBoundFromBytes(left_start),
        .end = try periodBoundFromBytes(left_end),
    };
    const right: PeriodSpan = .{
        .start = try periodBoundFromBytes(right_start),
        .end = try periodBoundFromBytes(right_end),
    };
    if (!periodBoundLessThan(left.start, left.end) or !periodBoundLessThan(right.start, right.end)) return error.InvalidColumnValue;
    return periodSpansOverlap(left, right);
}

pub fn temporalPeriodSpanBytesContainsPoint(
    encoded_start: []const u8,
    encoded_end: []const u8,
    encoded_point: []const u8,
) !bool {
    const span: PeriodSpan = .{
        .start = try periodBoundFromBytes(encoded_start),
        .end = try periodBoundFromBytes(encoded_end),
    };
    const point = try periodBoundFromBytes(encoded_point);
    if (!periodBoundLessThan(span.start, span.end)) return error.InvalidColumnValue;
    return periodSpanContainsPoint(span, point);
}

pub fn temporalPeriodBoundBytesFromJsonAlloc(
    alloc: Allocator,
    value_json: []const u8,
    column: schema_mod.RelationalColumn,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
    defer parsed.deinit();
    const bound: PeriodBound = switch (column.field_type) {
        .numeric => switch (parsed.value) {
            .integer => |value| .{ .f64_val = @floatFromInt(value) },
            .float => |value| .{ .f64_val = value },
            else => return error.InvalidColumnValue,
        },
        .datetime => switch (parsed.value) {
            .integer => |value| .{ .i64_val = value },
            else => return error.InvalidColumnValue,
        },
        else => return error.InvalidColumnValue,
    };
    return try periodBoundBytesAlloc(alloc, bound);
}

pub fn temporalPeriodStartBoundBytesFromJsonAlloc(
    alloc: Allocator,
    value_json: ?[]const u8,
    column: schema_mod.RelationalColumn,
) ![]u8 {
    const bound: PeriodBound = if (value_json) |json| blk: {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, json, .{}) catch return error.InvalidColumnValue;
        defer parsed.deinit();
        if (parsed.value == .null) {
            if (!column.nullable) return error.InvalidColumnValue;
            break :blk .neg_infinity;
        }
        break :blk switch (column.field_type) {
            .numeric => switch (parsed.value) {
                .integer => |value| .{ .f64_val = @floatFromInt(value) },
                .float => |value| .{ .f64_val = value },
                else => return error.InvalidColumnValue,
            },
            .datetime => switch (parsed.value) {
                .integer => |value| .{ .i64_val = value },
                else => return error.InvalidColumnValue,
            },
            else => return error.InvalidColumnValue,
        };
    } else blk: {
        if (!column.nullable) return error.InvalidColumnValue;
        break :blk .neg_infinity;
    };
    return try periodBoundBytesAlloc(alloc, bound);
}

pub fn temporalPeriodEndBoundBytesFromJsonAlloc(
    alloc: Allocator,
    value_json: ?[]const u8,
    column: schema_mod.RelationalColumn,
) ![]u8 {
    const bound: PeriodBound = if (value_json) |json| blk: {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, json, .{}) catch return error.InvalidColumnValue;
        defer parsed.deinit();
        if (parsed.value == .null) {
            if (!column.nullable) return error.InvalidColumnValue;
            break :blk .pos_infinity;
        }
        break :blk switch (column.field_type) {
            .numeric => switch (parsed.value) {
                .integer => |value| .{ .f64_val = @floatFromInt(value) },
                .float => |value| .{ .f64_val = value },
                else => return error.InvalidColumnValue,
            },
            .datetime => switch (parsed.value) {
                .integer => |value| .{ .i64_val = value },
                else => return error.InvalidColumnValue,
            },
            else => return error.InvalidColumnValue,
        };
    } else blk: {
        if (!column.nullable) return error.InvalidColumnValue;
        break :blk .pos_infinity;
    };
    return try periodBoundBytesAlloc(alloc, bound);
}

pub fn uniqueConstraintTupleValueAlloc(alloc: Allocator, row_value: []const u8, constraint: schema_mod.UniqueConstraint) !?[]u8 {
    return try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_value, constraint, &.{});
}

pub fn uniqueConstraintTupleValueWithColumnsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    constraint: schema_mod.UniqueConstraint,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (!(try rowMatchesUniqueConstraintPredicates(alloc, row_value, constraint.where, columns))) return null;
    if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, constraint.where_expressions, columns))) return null;
    const key_columns = if (std.mem.eql(u8, constraint.name, primary_key_constraint_name)) &.{} else columns;
    return try uniqueConstraintKeysTupleValueAlloc(alloc, row_value, constraint.columns, constraint.expressions, constraint.nulls_not_distinct, key_columns);
}

pub fn uniqueConstraintTupleValueWithExpressionValuesAlloc(
    alloc: Allocator,
    row_value: []const u8,
    constraint: schema_mod.UniqueConstraint,
    columns: []const schema_mod.RelationalColumn,
    expression_value_jsons: []const ?[]const u8,
) !?[]u8 {
    if (expression_value_jsons.len != constraint.expressions.len) return error.InvalidColumnValue;
    if (!(try rowMatchesUniqueConstraintPredicates(alloc, row_value, constraint.where, columns))) return null;
    if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, constraint.where_expressions, columns))) return null;
    const key_columns = if (std.mem.eql(u8, constraint.name, primary_key_constraint_name)) &.{} else columns;
    return try uniqueConstraintKeysTupleValueWithExpressionValuesAlloc(alloc, row_value, constraint.columns, constraint.expressions, constraint.nulls_not_distinct, key_columns, expression_value_jsons);
}

fn uniqueConstraintTupleValueWithCellsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    constraint: schema_mod.UniqueConstraint,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    if (!(try rowMatchesUniqueConstraintPredicatesFromCells(alloc, cells, constraint.where, columns))) return null;
    if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row_value, constraint.where_expressions, columns))) return null;
    const key_columns = if (std.mem.eql(u8, constraint.name, primary_key_constraint_name)) &.{} else columns;
    return try uniqueConstraintKeysTupleValueFromCellsAlloc(alloc, row_value, cells, constraint.columns, constraint.expressions, constraint.nulls_not_distinct, key_columns);
}

fn rowMatchesUniqueConstraintPredicates(
    alloc: Allocator,
    row_value: []const u8,
    predicates: []const schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    for (predicates) |predicate| {
        if (!(try rowMatchesUniqueConstraintPredicate(alloc, row_value, predicate, columns))) return false;
    }
    return true;
}

fn rowMatchesUniqueConstraintPredicatesFromCells(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    predicates: []const schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    for (predicates) |predicate| {
        if (!(try rowMatchesUniqueConstraintPredicateFromCells(alloc, cells, predicate, columns))) return false;
    }
    return true;
}

fn rowMatchesUniqueConstraintPredicate(
    alloc: Allocator,
    row_value: []const u8,
    predicate: schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    const cell = try relational_row_codec.findCellByPath(row_value, predicate.field);
    const column = findRelationalColumn(columns, predicate.field);
    const collation = if (column) |resolved| resolved.collation else null;
    return switch (predicate.op) {
        .is_null => cell == null,
        .is_not_null => cell != null,
        .eq => blk: {
            const present = cell orelse break :blk false;
            const value_json = predicate.value_json orelse return error.InvalidColumnValue;
            break :blk try cellEqualsJsonLiteralWithCollation(alloc, present, value_json, collation);
        },
        .ne => blk: {
            const present = cell orelse break :blk false;
            const value_json = predicate.value_json orelse return error.InvalidColumnValue;
            break :blk !(try cellEqualsJsonLiteralWithCollation(alloc, present, value_json, collation));
        },
    };
}

fn rowMatchesUniqueConstraintPredicateFromCells(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    predicate: schema_mod.UniquePredicate,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    const cell = findCellByColumnPathInCells(cells, predicate.field, columns);
    const column = findRelationalColumn(columns, predicate.field);
    const collation = if (column) |resolved| resolved.collation else null;
    return switch (predicate.op) {
        .is_null => cell == null,
        .is_not_null => cell != null,
        .eq => blk: {
            const present = cell orelse break :blk false;
            const value_json = predicate.value_json orelse return error.InvalidColumnValue;
            break :blk try cellEqualsJsonLiteralWithCollation(alloc, present, value_json, collation);
        },
        .ne => blk: {
            const present = cell orelse break :blk false;
            const value_json = predicate.value_json orelse return error.InvalidColumnValue;
            break :blk !(try cellEqualsJsonLiteralWithCollation(alloc, present, value_json, collation));
        },
    };
}

fn rowMatchesExpressionConditions(
    alloc: Allocator,
    row_value: []const u8,
    conditions: []const schema_mod.RelationalRowsExpressionCondition,
) !bool {
    return try rowMatchesExpressionConditionsWithColumns(alloc, row_value, conditions, &.{});
}

fn rowMatchesExpressionConditionsWithColumns(
    alloc: Allocator,
    row_value: []const u8,
    conditions: []const schema_mod.RelationalRowsExpressionCondition,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    for (conditions) |condition| {
        if (!(try rowMatchesExpressionConditionWithColumns(alloc, row_value, condition, columns))) return false;
    }
    return true;
}

fn rowMatchesExpressionCondition(
    alloc: Allocator,
    row_value: []const u8,
    condition: schema_mod.RelationalRowsExpressionCondition,
) !bool {
    return try rowMatchesExpressionConditionWithColumns(alloc, row_value, condition, &.{});
}

fn rowMatchesExpressionConditionWithColumns(
    alloc: Allocator,
    row_value: []const u8,
    condition: schema_mod.RelationalRowsExpressionCondition,
    columns: []const schema_mod.RelationalColumn,
) !bool {
    const lhs_json = try rowExpressionValueJsonAlloc(alloc, row_value, condition.lhs);
    defer alloc.free(lhs_json);
    var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return error.InvalidColumnValue;
    defer lhs.deinit();
    const condition_collation = expressionConditionDirectFieldCollation(columns, condition);
    return switch (condition.op) {
        .is_null => lhs.value == .null,
        .is_not_null => lhs.value != .null,
        .eq, .ne, .is_distinct, .is_not_distinct => blk: {
            if (condition.rhs.len != 1) return error.InvalidColumnValue;
            const rhs_json = try rowExpressionValueJsonAlloc(alloc, row_value, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidColumnValue;
            defer rhs.deinit();
            const equal = jsonValuesEqualWithCollation(lhs.value, rhs.value, condition_collation);
            break :blk switch (condition.op) {
                .eq, .is_not_distinct => equal,
                .ne, .is_distinct => !equal,
                else => unreachable,
            };
        },
        .gt, .gte, .lt, .lte => blk: {
            if (condition.rhs.len != 1) return error.InvalidColumnValue;
            const rhs_json = try rowExpressionValueJsonAlloc(alloc, row_value, condition.rhs[0]);
            defer alloc.free(rhs_json);
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidColumnValue;
            defer rhs.deinit();
            if (lhs.value == .null or rhs.value == .null) break :blk false;
            const comparison = compareJsonScalarsWithCollation(lhs.value, rhs.value, condition_collation) orelse return error.InvalidColumnValue;
            break :blk switch (condition.op) {
                .gt => comparison == .gt,
                .gte => comparison == .gt or comparison == .eq,
                .lt => comparison == .lt,
                .lte => comparison == .lt or comparison == .eq,
                else => unreachable,
            };
        },
    };
}

fn expressionConditionDirectFieldCollation(
    columns: []const schema_mod.RelationalColumn,
    condition: schema_mod.RelationalRowsExpressionCondition,
) ?[]const u8 {
    if (condition.rhs.len != 1) return null;
    if (condition.lhs.kind == .field and condition.lhs.field_source == .row and condition.rhs[0].kind == .value) {
        return relationalColumnCollation(columns, condition.lhs.field);
    }
    if (condition.rhs[0].kind == .field and condition.rhs[0].field_source == .row and condition.lhs.kind == .value) {
        return relationalColumnCollation(columns, condition.rhs[0].field);
    }
    return null;
}

fn relationalColumnCollation(columns: []const schema_mod.RelationalColumn, field: []const u8) ?[]const u8 {
    const column = findRelationalColumn(columns, field) orelse return null;
    return column.collation;
}

fn rowExpressionValueJsonAlloc(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.RelationalRowsExpression,
) anyerror![]u8 {
    if (expression.field_source != .row) return error.InvalidColumnValue;
    return switch (expression.kind) {
        .field => blk: {
            const cell = (try relational_row_codec.findCellByPath(row_value, expression.field)) orelse return try alloc.dupe(u8, "null");
            break :blk try cellJsonValueAlloc(alloc, cell);
        },
        .value => try alloc.dupe(u8, expression.value_json),
        .lower, .upper, .initcap, .md5, .soundex => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed.value != .string) return error.InvalidColumnValue;
            const transformed = switch (expression.kind) {
                .lower => try std.ascii.allocLowerString(alloc, parsed.value.string),
                .upper => try std.ascii.allocUpperString(alloc, parsed.value.string),
                .initcap => try initcapTextAlloc(alloc, parsed.value.string),
                .md5 => try md5HexTextAlloc(alloc, parsed.value.string),
                .soundex => try soundexTextAlloc(alloc, parsed.value.string),
                else => unreachable,
            };
            defer alloc.free(transformed);
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = transformed }, .{});
        },
        .trim, .ltrim, .rtrim => blk: {
            if (expression.operands.len != 1 and expression.operands.len != 2) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            if (source == null) break :blk try alloc.dupe(u8, "null");
            var trim_set: []const u8 = std.ascii.whitespace[0..];
            var trim_set_owned: ?[]u8 = null;
            defer if (trim_set_owned) |text| alloc.free(text);
            if (expression.operands.len == 2) {
                trim_set_owned = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
                if (trim_set_owned == null) break :blk try alloc.dupe(u8, "null");
                trim_set = trim_set_owned.?;
            }
            const transformed = try trimTextAlloc(alloc, source.?, trim_set, expression.kind != .rtrim, expression.kind != .ltrim);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .replace => blk: {
            if (expression.operands.len != 3) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const needle = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (needle) |text| alloc.free(text);
            const replacement = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[2]);
            defer if (replacement) |text| alloc.free(text);
            if (source == null or needle == null or replacement == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try replaceTextAlloc(alloc, source.?, needle.?, replacement.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .regexp_replace => blk: {
            if (expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const pattern = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (pattern) |text| alloc.free(text);
            const replacement = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[2]);
            defer if (replacement) |text| alloc.free(text);
            if (source == null or pattern == null or replacement == null) break :blk try alloc.dupe(u8, "null");
            var flags_text: []const u8 = "";
            var flags_owned: ?[]u8 = null;
            defer if (flags_owned) |text| alloc.free(text);
            if (expression.operands.len == 4) {
                flags_owned = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[3]);
                if (flags_owned == null) break :blk try alloc.dupe(u8, "null");
                flags_text = flags_owned.?;
            }
            const transformed = try regexpReplaceTextAlloc(alloc, source.?, pattern.?, replacement.?, flags_text);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .translate => blk: {
            if (expression.operands.len != 3) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const from_set = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (from_set) |text| alloc.free(text);
            const to_set = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[2]);
            defer if (to_set) |text| alloc.free(text);
            if (source == null or from_set == null or to_set == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try translateTextAlloc(alloc, source.?, from_set.?, to_set.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .substring => blk: {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const start = try rowExpressionIntegerValue(alloc, row_value, expression.operands[1]);
            if (source == null or start == null) break :blk try alloc.dupe(u8, "null");
            var length_count: ?i64 = null;
            if (expression.operands.len == 3) {
                length_count = try rowExpressionIntegerValue(alloc, row_value, expression.operands[2]);
                if (length_count == null) break :blk try alloc.dupe(u8, "null");
            }
            const transformed = try substringTextAlloc(alloc, source.?, start.?, length_count);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .overlay => blk: {
            if (expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const replacement = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (replacement) |text| alloc.free(text);
            const start = try rowExpressionIntegerValue(alloc, row_value, expression.operands[2]);
            if (source == null or replacement == null or start == null) break :blk try alloc.dupe(u8, "null");
            var length_count: ?i64 = null;
            if (expression.operands.len == 4) {
                length_count = try rowExpressionIntegerValue(alloc, row_value, expression.operands[3]);
                if (length_count == null) break :blk try alloc.dupe(u8, "null");
            }
            const transformed = try overlayTextAlloc(alloc, source.?, replacement.?, start.?, length_count);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .split_part => blk: {
            if (expression.operands.len != 3) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const delimiter = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (delimiter) |text| alloc.free(text);
            const field_index = try rowExpressionIntegerValue(alloc, row_value, expression.operands[2]);
            if (source == null or delimiter == null or field_index == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try splitPartTextAlloc(alloc, source.?, delimiter.?, field_index.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .left, .right => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const count = try rowExpressionIntegerValue(alloc, row_value, expression.operands[1]);
            if (source == null or count == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try leftRightTextAlloc(alloc, source.?, count.?, expression.kind == .left);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .lpad, .rpad => blk: {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const target_count = try rowExpressionIntegerValue(alloc, row_value, expression.operands[1]);
            if (source == null or target_count == null) break :blk try alloc.dupe(u8, "null");
            var fill_text: []const u8 = " ";
            var fill_owned: ?[]u8 = null;
            defer if (fill_owned) |text| alloc.free(text);
            if (expression.operands.len == 3) {
                fill_owned = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[2]);
                if (fill_owned == null) break :blk try alloc.dupe(u8, "null");
                fill_text = fill_owned.?;
            }
            const transformed = try padTextAlloc(alloc, source.?, target_count.?, fill_text, expression.kind == .lpad);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .repeat => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const count = try rowExpressionIntegerValue(alloc, row_value, expression.operands[1]);
            if (source == null or count == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try repeatTextAlloc(alloc, source.?, count.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .reverse => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            if (source == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try reverseTextAlloc(alloc, source.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .starts_with, .ends_with => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const needle = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (needle) |text| alloc.free(text);
            if (source == null or needle == null) break :blk try alloc.dupe(u8, "null");
            const matched = if (expression.kind == .starts_with)
                std.mem.startsWith(u8, source.?, needle.?)
            else
                std.mem.endsWith(u8, source.?, needle.?);
            break :blk try alloc.dupe(u8, if (matched) "true" else "false");
        },
        .like, .ilike => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            const pattern = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
            defer if (pattern) |text| alloc.free(text);
            if (source == null or pattern == null) break :blk try alloc.dupe(u8, "null");
            const matched = sqlLikePatternMatches(source.?, pattern.?, expression.kind == .ilike);
            break :blk try alloc.dupe(u8, if (matched) "true" else "false");
        },
        .bool_and, .bool_or => blk: {
            if (expression.operands.len < 2) return error.InvalidColumnValue;
            var saw_null = false;
            for (expression.operands) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null => saw_null = true,
                    .bool => |value| switch (expression.kind) {
                        .bool_and => if (!value) break :blk try alloc.dupe(u8, "false"),
                        .bool_or => if (value) break :blk try alloc.dupe(u8, "true"),
                        else => unreachable,
                    },
                    else => return error.InvalidColumnValue,
                }
            }
            if (saw_null) break :blk try alloc.dupe(u8, "null");
            break :blk try alloc.dupe(u8, if (expression.kind == .bool_and) "true" else "false");
        },
        .bool_not => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .bool => |value| break :blk try alloc.dupe(u8, if (value) "false" else "true"),
                else => return error.InvalidColumnValue,
            }
        },
        .length, .octet_length, .bit_length, .strpos, .ascii => blk: {
            const source = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[0]);
            defer if (source) |text| alloc.free(text);
            if (source == null) break :blk try alloc.dupe(u8, "null");
            const result: i64 = switch (expression.kind) {
                .length => @intCast(std.unicode.utf8CountCodepoints(source.?) catch return error.InvalidColumnValue),
                .octet_length => @intCast(source.?.len),
                .bit_length => @intCast(source.?.len * 8),
                .strpos => pos: {
                    if (expression.operands.len != 2) return error.InvalidColumnValue;
                    const needle = try rowExpressionStringValueAlloc(alloc, row_value, expression.operands[1]);
                    defer if (needle) |text| alloc.free(text);
                    if (needle == null) break :blk try alloc.dupe(u8, "null");
                    break :pos @intCast(try strposTextCodepointPosition(source.?, needle.?));
                },
                .ascii => if (source.?.len == 0) 0 else @intCast(try firstUtf8Codepoint(source.?)),
                else => unreachable,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{result});
        },
        .chr => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const codepoint = try rowExpressionIntegerValue(alloc, row_value, expression.operands[0]);
            if (codepoint == null) break :blk try alloc.dupe(u8, "null");
            const transformed = try codepointTextAlloc(alloc, codepoint.?);
            defer alloc.free(transformed);
            break :blk try jsonStringAlloc(alloc, transformed);
        },
        .json_extract => blk: {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidColumnValue;
            const root_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(root_json);
            var root = std.json.parseFromSlice(std.json.Value, alloc, root_json, .{}) catch return error.InvalidColumnValue;
            defer root.deinit();
            const selected = jsonValueAtDotPath(root.value, expression.json_path) orelse break :blk try alloc.dupe(u8, "null");
            if (!expression.json_as_text) break :blk try std.json.Stringify.valueAlloc(alloc, selected.*, .{});
            break :blk try jsonExtractTextValueJsonAlloc(alloc, selected.*);
        },
        .json_path_exists => blk: {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidColumnValue;
            const root_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(root_json);
            var root = std.json.parseFromSlice(std.json.Value, alloc, root_json, .{}) catch return error.InvalidColumnValue;
            defer root.deinit();
            break :blk try alloc.dupe(u8, if (jsonValueAtDotPath(root.value, expression.json_path) != null) "true" else "false");
        },
        .json_build_object => try rowExpressionJsonBuildObjectValueJsonAlloc(alloc, row_value, expression),
        .to_jsonb => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            var transferred = false;
            errdefer if (!transferred) alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            transferred = true;
            break :blk value_json;
        },
        .json_typeof => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            const type_name = switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .bool => "boolean",
                .integer, .float, .number_string => "number",
                .string => "string",
                .array => "array",
                .object => "object",
            };
            break :blk try jsonStringAlloc(alloc, type_name);
        },
        .json_array_length => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .array => |array| break :blk try std.fmt.allocPrint(alloc, "{d}", .{array.items.len}),
                else => return error.InvalidColumnValue,
            }
        },
        .array_length => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            switch (parsed.value) {
                .null => break :blk try alloc.dupe(u8, "null"),
                .array => |array| break :blk try std.fmt.allocPrint(alloc, "{d}", .{array.items.len}),
                else => return error.InvalidColumnValue,
            }
        },
        .array_position => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const array_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(array_json);
            const needle_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(needle_json);
            var parsed_array = std.json.parseFromSlice(std.json.Value, alloc, array_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_array.deinit();
            var parsed_needle = std.json.parseFromSlice(std.json.Value, alloc, needle_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_needle.deinit();
            if (parsed_array.value == .null or parsed_needle.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_array.value != .array) return error.InvalidColumnValue;
            for (parsed_array.value.array.items, 0..) |item, index| {
                if (jsonValuesEqualExact(item, parsed_needle.value)) {
                    break :blk try std.fmt.allocPrint(alloc, "{d}", .{index + 1});
                }
            }
            break :blk try alloc.dupe(u8, "null");
        },
        .array_positions => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const array_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(array_json);
            const needle_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(needle_json);
            var parsed_array = std.json.parseFromSlice(std.json.Value, alloc, array_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_array.deinit();
            var parsed_needle = std.json.parseFromSlice(std.json.Value, alloc, needle_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_needle.deinit();
            if (parsed_array.value == .null or parsed_needle.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_array.value != .array) return error.InvalidColumnValue;
            break :blk try arrayPositionsValueJsonAlloc(alloc, parsed_array.value.array.items, parsed_needle.value);
        },
        .array_append, .array_prepend, .array_remove => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const array_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(array_json);
            const element_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(element_json);
            var parsed_array = std.json.parseFromSlice(std.json.Value, alloc, array_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_array.deinit();
            var parsed_element = std.json.parseFromSlice(std.json.Value, alloc, element_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_element.deinit();
            if (parsed_array.value == .null or parsed_element.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_array.value != .array) return error.InvalidColumnValue;
            break :blk try arrayElementTransformValueJsonAlloc(alloc, parsed_array.value.array.items, parsed_element.value, expression.kind);
        },
        .array_cat => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const left_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(left_json);
            const right_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(right_json);
            var parsed_left = std.json.parseFromSlice(std.json.Value, alloc, left_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_left.deinit();
            var parsed_right = std.json.parseFromSlice(std.json.Value, alloc, right_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_right.deinit();
            if (parsed_left.value == .null or parsed_right.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_left.value != .array or parsed_right.value != .array) return error.InvalidColumnValue;
            break :blk try arrayConcatValueJsonAlloc(alloc, parsed_left.value.array.items, parsed_right.value.array.items);
        },
        .array_replace => blk: {
            if (expression.operands.len != 3) return error.InvalidColumnValue;
            const array_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(array_json);
            const old_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(old_json);
            const new_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[2]);
            defer alloc.free(new_json);
            var parsed_array = std.json.parseFromSlice(std.json.Value, alloc, array_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_array.deinit();
            var parsed_old = std.json.parseFromSlice(std.json.Value, alloc, old_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_old.deinit();
            var parsed_new = std.json.parseFromSlice(std.json.Value, alloc, new_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_new.deinit();
            if (parsed_array.value == .null or parsed_old.value == .null or parsed_new.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_array.value != .array) return error.InvalidColumnValue;
            break :blk try arrayReplaceValueJsonAlloc(alloc, parsed_array.value.array.items, parsed_old.value, parsed_new.value);
        },
        .array_to_string => blk: {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidColumnValue;
            const array_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(array_json);
            const delimiter_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(delimiter_json);
            var parsed_array = std.json.parseFromSlice(std.json.Value, alloc, array_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_array.deinit();
            var parsed_delimiter = std.json.parseFromSlice(std.json.Value, alloc, delimiter_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_delimiter.deinit();
            if (parsed_array.value == .null or parsed_delimiter.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_array.value != .array or parsed_delimiter.value != .string) return error.InvalidColumnValue;
            var null_text: ?[]const u8 = null;
            if (expression.operands.len == 3) {
                const null_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[2]);
                defer alloc.free(null_json);
                var parsed_null = std.json.parseFromSlice(std.json.Value, alloc, null_json, .{}) catch return error.InvalidColumnValue;
                defer parsed_null.deinit();
                if (parsed_null.value == .null) break :blk try alloc.dupe(u8, "null");
                if (parsed_null.value != .string) return error.InvalidColumnValue;
                null_text = parsed_null.value.string;
            }
            break :blk try arrayToStringValueJsonAlloc(alloc, parsed_array.value.array.items, parsed_delimiter.value.string, null_text);
        },
        .string_to_array => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            const delimiter_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(delimiter_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            var delimiter = std.json.parseFromSlice(std.json.Value, alloc, delimiter_json, .{}) catch return error.InvalidColumnValue;
            defer delimiter.deinit();
            if (parsed.value == .null or delimiter.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed.value != .string or delimiter.value != .string or delimiter.value.string.len == 0) return error.InvalidColumnValue;
            break :blk try stringToArrayValueJsonAlloc(alloc, parsed.value.string, delimiter.value.string);
        },
        .concat => blk: {
            if (expression.operands.len == 0) return error.InvalidColumnValue;
            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            for (expression.operands) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                const text = try rowScalarJsonValueTextAlloc(alloc, parsed.value);
                defer alloc.free(text);
                try joined.appendSlice(alloc, text);
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .coalesce => blk: {
            if (expression.operands.len == 0) return error.InvalidColumnValue;
            for (expression.operands) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch {
                    alloc.free(value_json);
                    return error.InvalidColumnValue;
                };
                defer parsed.deinit();
                if (parsed.value != .null) break :blk value_json;
                alloc.free(value_json);
            }
            break :blk try alloc.dupe(u8, "null");
        },
        .nullif => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const lhs_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            var lhs_transferred = false;
            errdefer if (!lhs_transferred) alloc.free(lhs_json);
            const rhs_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(rhs_json);
            var lhs = std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{}) catch return error.InvalidColumnValue;
            defer lhs.deinit();
            var rhs = std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{}) catch return error.InvalidColumnValue;
            defer rhs.deinit();
            if (lhs.value != .null and rhs.value != .null and jsonValuesEqualExact(lhs.value, rhs.value)) {
                alloc.free(lhs_json);
                lhs_transferred = true;
                break :blk try alloc.dupe(u8, "null");
            }
            lhs_transferred = true;
            break :blk lhs_json;
        },
        .greatest, .least => blk: {
            if (expression.operands.len == 0) return error.InvalidColumnValue;
            var best_json: ?[]u8 = null;
            errdefer if (best_json) |owned| alloc.free(owned);
            var best_value: ?std.json.Parsed(std.json.Value) = null;
            defer if (best_value) |*parsed| parsed.deinit();

            for (expression.operands) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                var value_transferred = false;
                errdefer if (!value_transferred) alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
                var parsed_transferred = false;
                defer if (!parsed_transferred) parsed.deinit();
                if (parsed.value == .null) {
                    alloc.free(value_json);
                    value_transferred = true;
                    continue;
                }
                if (best_value) |*current| {
                    const comparison = compareJsonScalars(parsed.value, current.value) orelse return error.InvalidColumnValue;
                    const replace = switch (expression.kind) {
                        .greatest => comparison == .gt,
                        .least => comparison == .lt,
                        else => unreachable,
                    };
                    if (!replace) {
                        alloc.free(value_json);
                        value_transferred = true;
                        continue;
                    }
                    current.deinit();
                    alloc.free(best_json.?);
                }
                best_json = value_json;
                best_value = parsed;
                value_transferred = true;
                parsed_transferred = true;
            }
            break :blk if (best_json) |owned| owned else try alloc.dupe(u8, "null");
        },
        .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            const value = jsonValueAsFloat(parsed.value) orelse return error.InvalidColumnValue;
            if (expression.kind == .sqrt and value < 0) return error.InvalidColumnValue;
            const result = switch (expression.kind) {
                .abs => if (value < 0) -value else value,
                .round => @round(value),
                .trunc => @trunc(value),
                .floor => @floor(value),
                .ceil => @ceil(value),
                .sqrt => @sqrt(value),
                .sign => if (value < 0) @as(f64, -1) else if (value > 0) @as(f64, 1) else @as(f64, 0),
                else => unreachable,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{result});
        },
        .add, .sub, .mul, .div, .mod, .power => blk: {
            if ((expression.kind == .add or expression.kind == .mul) and expression.operands.len < 2) return error.InvalidColumnValue;
            if ((expression.kind == .sub or expression.kind == .div or expression.kind == .mod or expression.kind == .power) and expression.operands.len != 2) return error.InvalidColumnValue;
            if ((expression.kind == .add or expression.kind == .sub) and expression.operands.len == 2 and rowExpressionHasDirectCalendarIntervalOperand(expression)) {
                break :blk try rowExpressionEvaluateCalendarIntervalArithmeticAlloc(alloc, row_value, expression);
            }
            const first_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(first_json);
            var first = std.json.parseFromSlice(std.json.Value, alloc, first_json, .{}) catch return error.InvalidColumnValue;
            defer first.deinit();
            if (first.value == .null) break :blk try alloc.dupe(u8, "null");
            var result = jsonValueAsFloat(first.value) orelse return error.InvalidColumnValue;
            for (expression.operands[1..]) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
                defer parsed.deinit();
                if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
                const rhs = jsonValueAsFloat(parsed.value) orelse return error.InvalidColumnValue;
                result = switch (expression.kind) {
                    .add => result + rhs,
                    .sub => result - rhs,
                    .mul => result * rhs,
                    .div => if (rhs == 0) return error.InvalidColumnValue else result / rhs,
                    .mod => if (rhs == 0) return error.InvalidColumnValue else result - @trunc(result / rhs) * rhs,
                    .power => std.math.pow(f64, result, rhs),
                    else => unreachable,
                };
                if (!std.math.isFinite(result)) return error.InvalidColumnValue;
            }
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{result});
        },
        .interval_ns, .interval_months => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            break :blk try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
        },
        .date_trunc => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const unit_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(unit_json);
            const timestamp_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(timestamp_json);
            var parsed_unit = std.json.parseFromSlice(std.json.Value, alloc, unit_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_unit.deinit();
            var parsed_timestamp = std.json.parseFromSlice(std.json.Value, alloc, timestamp_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_timestamp.deinit();
            if (parsed_unit.value == .null or parsed_timestamp.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_unit.value != .string) return error.InvalidColumnValue;
            const timestamp_ns = jsonValueAsU64(parsed_timestamp.value) orelse return error.InvalidColumnValue;
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{try rowExpressionDateTruncUtcTimestampNs(parsed_unit.value.string, timestamp_ns)});
        },
        .date_bin => blk: {
            if (expression.operands.len != 3) return error.InvalidColumnValue;
            const stride_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(stride_json);
            const timestamp_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(timestamp_json);
            const origin_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[2]);
            defer alloc.free(origin_json);
            var parsed_stride = std.json.parseFromSlice(std.json.Value, alloc, stride_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_stride.deinit();
            var parsed_timestamp = std.json.parseFromSlice(std.json.Value, alloc, timestamp_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_timestamp.deinit();
            var parsed_origin = std.json.parseFromSlice(std.json.Value, alloc, origin_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_origin.deinit();
            if (parsed_stride.value == .null or parsed_timestamp.value == .null or parsed_origin.value == .null) break :blk try alloc.dupe(u8, "null");
            const stride_ns = jsonValueAsU64(parsed_stride.value) orelse return error.InvalidColumnValue;
            const timestamp_ns = jsonValueAsU64(parsed_timestamp.value) orelse return error.InvalidColumnValue;
            const origin_ns = jsonValueAsU64(parsed_origin.value) orelse return error.InvalidColumnValue;
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{try rowExpressionDateBinUtcTimestampNs(stride_ns, timestamp_ns, origin_ns)});
        },
        .date_part => blk: {
            if (expression.operands.len != 2) return error.InvalidColumnValue;
            const unit_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(unit_json);
            const timestamp_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[1]);
            defer alloc.free(timestamp_json);
            var parsed_unit = std.json.parseFromSlice(std.json.Value, alloc, unit_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_unit.deinit();
            var parsed_timestamp = std.json.parseFromSlice(std.json.Value, alloc, timestamp_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_timestamp.deinit();
            if (parsed_unit.value == .null or parsed_timestamp.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_unit.value != .string) return error.InvalidColumnValue;
            const timestamp_ns = jsonValueAsU64(parsed_timestamp.value) orelse return error.InvalidColumnValue;
            break :blk try rowExpressionDatePartUtcTimestampJsonAlloc(alloc, parsed_unit.value.string, timestamp_ns);
        },
        .cast => blk: {
            if (expression.operands.len != 1) return error.InvalidColumnValue;
            const cast_type = expression.cast_type orelse return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(value_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
            defer parsed.deinit();
            if (parsed.value == .null) break :blk try alloc.dupe(u8, "null");
            break :blk try castRowExpressionValueJsonAlloc(alloc, parsed.value, cast_type);
        },
        .concat_ws => blk: {
            if (expression.operands.len < 2) return error.InvalidColumnValue;
            const separator_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[0]);
            defer alloc.free(separator_json);
            var parsed_separator = std.json.parseFromSlice(std.json.Value, alloc, separator_json, .{}) catch return error.InvalidColumnValue;
            defer parsed_separator.deinit();
            if (parsed_separator.value == .null) break :blk try alloc.dupe(u8, "null");
            if (parsed_separator.value != .string) return error.InvalidColumnValue;

            var joined = std.ArrayListUnmanaged(u8).empty;
            defer joined.deinit(alloc);
            var emitted = false;
            for (expression.operands[1..]) |operand| {
                const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, operand);
                defer alloc.free(value_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
                defer parsed.deinit();
                if (parsed.value == .null) continue;
                if (parsed.value != .string) return error.InvalidColumnValue;
                if (emitted) try joined.appendSlice(alloc, parsed_separator.value.string);
                try joined.appendSlice(alloc, parsed.value.string);
                emitted = true;
            }
            break :blk try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = joined.items }, .{});
        },
        .case => blk: {
            if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.InvalidColumnValue;
            for (expression.case_branches) |branch| {
                if (try rowMatchesExpressionCondition(alloc, row_value, branch.when)) {
                    break :blk try rowExpressionValueJsonAlloc(alloc, row_value, branch.then);
                }
            }
            break :blk try rowExpressionValueJsonAlloc(alloc, row_value, expression.case_else[0]);
        },
        else => return error.InvalidColumnValue,
    };
}

fn rowExpressionStringValueAlloc(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.RelationalRowsExpression,
) anyerror!?[]u8 {
    const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression);
    defer alloc.free(value_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
    defer parsed.deinit();
    return switch (parsed.value) {
        .null => null,
        .string => |text| try alloc.dupe(u8, text),
        else => error.InvalidColumnValue,
    };
}

fn rowExpressionIntegerValue(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.RelationalRowsExpression,
) anyerror!?i64 {
    const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression);
    defer alloc.free(value_json);
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
    defer parsed.deinit();
    return switch (parsed.value) {
        .null => null,
        .integer => |integer| integer,
        .number_string => |text| std.fmt.parseInt(i64, text, 10) catch return error.InvalidColumnValue,
        else => error.InvalidColumnValue,
    };
}

fn jsonStringAlloc(alloc: Allocator, value: []const u8) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = value }, .{});
}

fn jsonExtractTextValueJsonAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, "null"),
        .string => |text| try jsonStringAlloc(alloc, text),
        else => blk: {
            const text = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(text);
            break :blk try jsonStringAlloc(alloc, text);
        },
    };
}

fn rowScalarJsonValueTextAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .string => |text| try alloc.dupe(u8, text),
        .integer => |integer| try std.fmt.allocPrint(alloc, "{d}", .{integer}),
        .float => |float| try std.fmt.allocPrint(alloc, "{d}", .{float}),
        .number_string => |text| try alloc.dupe(u8, text),
        .bool => |enabled| try alloc.dupe(u8, if (enabled) "true" else "false"),
        else => error.InvalidColumnValue,
    };
}

fn castRowExpressionValueJsonAlloc(
    alloc: Allocator,
    value: std.json.Value,
    cast_type: schema_mod.RelationalRowsExpressionCastType,
) ![]u8 {
    return switch (cast_type) {
        .text => blk: {
            const text = try rowScalarJsonValueTextAlloc(alloc, value);
            defer alloc.free(text);
            break :blk try jsonStringAlloc(alloc, text);
        },
        .numeric => blk: {
            const number = switch (value) {
                .integer, .float, .number_string => jsonValueAsFloat(value) orelse return error.InvalidColumnValue,
                .string => |text| std.fmt.parseFloat(f64, text) catch return error.InvalidColumnValue,
                else => return error.InvalidColumnValue,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{number});
        },
        .datetime => blk: {
            const timestamp_ns = switch (value) {
                .integer => |integer| if (integer >= 0) @as(u64, @intCast(integer)) else return error.InvalidColumnValue,
                .number_string => |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidColumnValue,
                .string => |text| std.fmt.parseInt(u64, text, 10) catch return error.InvalidColumnValue,
                else => return error.InvalidColumnValue,
            };
            break :blk try std.fmt.allocPrint(alloc, "{d}", .{timestamp_ns});
        },
        .bool => blk: {
            const enabled = switch (value) {
                .bool => |enabled| enabled,
                .string => |text| if (std.mem.eql(u8, text, "true"))
                    true
                else if (std.mem.eql(u8, text, "false"))
                    false
                else
                    return error.InvalidColumnValue,
                else => return error.InvalidColumnValue,
            };
            break :blk try alloc.dupe(u8, if (enabled) "true" else "false");
        },
    };
}

fn rowExpressionJsonBuildObjectValueJsonAlloc(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.RelationalRowsExpression,
) ![]u8 {
    if (expression.operands.len % 2 != 0) return error.InvalidColumnValue;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    var first = true;
    var index: usize = 0;
    while (index < expression.operands.len) : (index += 2) {
        const key_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[index]);
        defer alloc.free(key_json);
        var parsed_key = std.json.parseFromSlice(std.json.Value, alloc, key_json, .{}) catch return error.InvalidColumnValue;
        defer parsed_key.deinit();
        if (parsed_key.value != .string) return error.InvalidColumnValue;

        const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, expression.operands[index + 1]);
        defer alloc.free(value_json);
        var parsed_value = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
        defer parsed_value.deinit();
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}:", .{std.json.fmt(parsed_key.value.string, .{})});
        try std.json.Stringify.value(parsed_value.value, .{}, writer);
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

fn rowExpressionJsonValueTextAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .string => |text| try alloc.dupe(u8, text),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .integer => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .float => |number| try std.fmt.allocPrint(alloc, "{d}", .{number}),
        .number_string => |number| try alloc.dupe(u8, number),
        .array, .object => try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn arrayPositionsValueJsonAlloc(
    alloc: Allocator,
    items: []const std.json.Value,
    needle: std.json.Value,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var first = true;
    for (items, 0..) |item, index| {
        if (!jsonValuesEqualExact(item, needle)) continue;
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{d}", .{index + 1});
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn arrayElementTransformValueJsonAlloc(
    alloc: Allocator,
    items: []const std.json.Value,
    element: std.json.Value,
    kind: schema_mod.RelationalRowsExpressionKind,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var first = true;
    if (kind == .array_prepend) {
        try std.json.Stringify.value(element, .{}, writer);
        for (items) |item| {
            try writer.writeByte(',');
            try std.json.Stringify.value(item, .{}, writer);
        }
    } else if (kind == .array_append) {
        for (items) |item| {
            if (!first) try writer.writeByte(',');
            first = false;
            try std.json.Stringify.value(item, .{}, writer);
        }
        if (!first) try writer.writeByte(',');
        try std.json.Stringify.value(element, .{}, writer);
    } else if (kind == .array_remove) {
        for (items) |item| {
            if (jsonValuesEqualExact(item, element)) continue;
            if (!first) try writer.writeByte(',');
            first = false;
            try std.json.Stringify.value(item, .{}, writer);
        }
    } else return error.InvalidColumnValue;
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn arrayConcatValueJsonAlloc(
    alloc: Allocator,
    left_items: []const std.json.Value,
    right_items: []const std.json.Value,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var first = true;
    for (left_items) |item| {
        if (!first) try writer.writeByte(',');
        first = false;
        try std.json.Stringify.value(item, .{}, writer);
    }
    for (right_items) |item| {
        if (!first) try writer.writeByte(',');
        first = false;
        try std.json.Stringify.value(item, .{}, writer);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn arrayReplaceValueJsonAlloc(
    alloc: Allocator,
    items: []const std.json.Value,
    old_value: std.json.Value,
    new_value: std.json.Value,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var first = true;
    for (items) |item| {
        if (!first) try writer.writeByte(',');
        first = false;
        try std.json.Stringify.value(if (jsonValuesEqualExact(item, old_value)) new_value else item, .{}, writer);
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn arrayToStringValueJsonAlloc(
    alloc: Allocator,
    items: []const std.json.Value,
    delimiter: []const u8,
    null_text: ?[]const u8,
) ![]u8 {
    var joined: std.Io.Writer.Allocating = .init(alloc);
    errdefer joined.deinit();
    const writer = &joined.writer;
    var first = true;
    for (items) |item| {
        if (item == .null and null_text == null) continue;
        if (!first) try writer.writeAll(delimiter);
        first = false;
        if (item == .null) {
            try writer.writeAll(null_text.?);
        } else {
            const text = try rowExpressionJsonValueTextAlloc(alloc, item);
            defer alloc.free(text);
            try writer.writeAll(text);
        }
    }
    const joined_text = try joined.toOwnedSlice();
    defer alloc.free(joined_text);
    return try jsonStringAlloc(alloc, joined_text);
}

fn stringToArrayValueJsonAlloc(
    alloc: Allocator,
    text: []const u8,
    delimiter: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var split = std.mem.splitSequence(u8, text, delimiter);
    var first = true;
    while (split.next()) |part| {
        if (!first) try writer.writeByte(',');
        first = false;
        try writer.print("{f}", .{std.json.fmt(part, .{})});
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn rowExpressionHasDirectCalendarIntervalOperand(expression: schema_mod.RelationalRowsExpression) bool {
    for (expression.operands) |operand| {
        if (operand.kind == .interval_months) return true;
    }
    return false;
}

fn rowExpressionEvaluateCalendarIntervalArithmeticAlloc(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.RelationalRowsExpression,
) ![]u8 {
    if ((expression.kind != .add and expression.kind != .sub) or expression.operands.len != 2) return error.InvalidColumnValue;
    const lhs_is_interval = expression.operands[0].kind == .interval_months;
    const rhs_is_interval = expression.operands[1].kind == .interval_months;
    if (lhs_is_interval == rhs_is_interval) return error.InvalidColumnValue;
    if (expression.kind == .sub and lhs_is_interval) return error.InvalidColumnValue;

    const timestamp_expression = if (lhs_is_interval) expression.operands[1] else expression.operands[0];
    const months_expression = if (lhs_is_interval) expression.operands[0] else expression.operands[1];
    const timestamp_json = try rowExpressionValueJsonAlloc(alloc, row_value, timestamp_expression);
    defer alloc.free(timestamp_json);
    const months_json = try rowExpressionValueJsonAlloc(alloc, row_value, months_expression);
    defer alloc.free(months_json);

    var parsed_timestamp = std.json.parseFromSlice(std.json.Value, alloc, timestamp_json, .{}) catch return error.InvalidColumnValue;
    defer parsed_timestamp.deinit();
    var parsed_months = std.json.parseFromSlice(std.json.Value, alloc, months_json, .{}) catch return error.InvalidColumnValue;
    defer parsed_months.deinit();
    if (parsed_timestamp.value == .null or parsed_months.value == .null) return try alloc.dupe(u8, "null");
    const timestamp_ns = jsonValueAsU64(parsed_timestamp.value) orelse return error.InvalidColumnValue;
    var months = jsonValueAsI64(parsed_months.value) orelse return error.InvalidColumnValue;
    if (expression.kind == .sub) months = -months;
    return try std.fmt.allocPrint(alloc, "{d}", .{try rowExpressionAddUtcMonthsToTimestampNs(timestamp_ns, months)});
}

const row_expression_ns_per_day: u64 = 86_400_000_000_000;
const row_expression_ns_per_hour: u64 = 3_600_000_000_000;
const row_expression_ns_per_minute: u64 = 60_000_000_000;
const row_expression_ns_per_second: u64 = 1_000_000_000;
const row_expression_ns_per_millisecond: u64 = 1_000_000;
const row_expression_ns_per_microsecond: u64 = 1_000;

fn rowExpressionDateTruncUtcTimestampNs(unit: []const u8, timestamp_ns: u64) !u64 {
    if (std.ascii.eqlIgnoreCase(unit, "microsecond") or std.ascii.eqlIgnoreCase(unit, "microseconds")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_microsecond);
    if (std.ascii.eqlIgnoreCase(unit, "millisecond") or std.ascii.eqlIgnoreCase(unit, "milliseconds")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_millisecond);
    if (std.ascii.eqlIgnoreCase(unit, "second") or std.ascii.eqlIgnoreCase(unit, "seconds")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_second);
    if (std.ascii.eqlIgnoreCase(unit, "minute") or std.ascii.eqlIgnoreCase(unit, "minutes")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_minute);
    if (std.ascii.eqlIgnoreCase(unit, "hour") or std.ascii.eqlIgnoreCase(unit, "hours")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_hour);
    if (std.ascii.eqlIgnoreCase(unit, "day") or std.ascii.eqlIgnoreCase(unit, "days")) return timestamp_ns - (timestamp_ns % row_expression_ns_per_day);

    const day_count: i64 = @intCast(timestamp_ns / row_expression_ns_per_day);
    if (std.ascii.eqlIgnoreCase(unit, "week") or std.ascii.eqlIgnoreCase(unit, "weeks")) {
        const days_since_monday = @mod(day_count + 3, 7);
        const start_day = day_count - days_since_monday;
        if (start_day < 0) return error.InvalidColumnValue;
        return std.math.mul(u64, @intCast(start_day), row_expression_ns_per_day) catch return error.InvalidColumnValue;
    }

    var civil = rowExpressionCivilFromDays(day_count);
    if (std.ascii.eqlIgnoreCase(unit, "month") or std.ascii.eqlIgnoreCase(unit, "months")) {
        civil.day = 1;
    } else if (std.ascii.eqlIgnoreCase(unit, "quarter") or std.ascii.eqlIgnoreCase(unit, "quarters")) {
        civil.month = @intCast(@divFloor(@as(u16, civil.month) - 1, 3) * 3 + 1);
        civil.day = 1;
    } else if (std.ascii.eqlIgnoreCase(unit, "year") or std.ascii.eqlIgnoreCase(unit, "years")) {
        civil.month = 1;
        civil.day = 1;
    } else if (std.ascii.eqlIgnoreCase(unit, "decade") or std.ascii.eqlIgnoreCase(unit, "decades")) {
        civil.year = @divFloor(civil.year, 10) * 10;
        civil.month = 1;
        civil.day = 1;
    } else if (std.ascii.eqlIgnoreCase(unit, "century") or std.ascii.eqlIgnoreCase(unit, "centuries")) {
        civil.year = @divFloor(civil.year - 1, 100) * 100 + 1;
        civil.month = 1;
        civil.day = 1;
    } else if (std.ascii.eqlIgnoreCase(unit, "millennium") or std.ascii.eqlIgnoreCase(unit, "millennia") or std.ascii.eqlIgnoreCase(unit, "millenniums")) {
        civil.year = @divFloor(civil.year - 1, 1000) * 1000 + 1;
        civil.month = 1;
        civil.day = 1;
    } else return error.InvalidColumnValue;
    const start_day = rowExpressionDaysFromCivil(civil.year, civil.month, civil.day);
    if (start_day < 0) return error.InvalidColumnValue;
    return std.math.mul(u64, @intCast(start_day), row_expression_ns_per_day) catch return error.InvalidColumnValue;
}

fn rowExpressionDateBinUtcTimestampNs(stride_ns: u64, timestamp_ns: u64, origin_ns: u64) !u64 {
    if (stride_ns == 0) return error.InvalidColumnValue;
    const stride: i128 = @intCast(stride_ns);
    const timestamp: i128 = @intCast(timestamp_ns);
    const origin: i128 = @intCast(origin_ns);
    const bucket = origin + @divFloor(timestamp - origin, stride) * stride;
    if (bucket < 0 or bucket > std.math.maxInt(u64)) return error.InvalidColumnValue;
    return @intCast(bucket);
}

fn rowExpressionDatePartUtcTimestampJsonAlloc(alloc: Allocator, unit: []const u8, timestamp_ns: u64) ![]u8 {
    if (std.ascii.eqlIgnoreCase(unit, "epoch")) {
        const seconds = @as(f64, @floatFromInt(timestamp_ns)) / @as(f64, @floatFromInt(row_expression_ns_per_second));
        return try std.fmt.allocPrint(alloc, "{d}", .{seconds});
    }

    const day_count: i64 = @intCast(timestamp_ns / row_expression_ns_per_day);
    const day_ns = timestamp_ns % row_expression_ns_per_day;
    const civil = rowExpressionCivilFromDays(day_count);
    if (std.ascii.eqlIgnoreCase(unit, "year") or std.ascii.eqlIgnoreCase(unit, "years")) return try std.fmt.allocPrint(alloc, "{d}", .{civil.year});
    if (std.ascii.eqlIgnoreCase(unit, "decade") or std.ascii.eqlIgnoreCase(unit, "decades")) return try std.fmt.allocPrint(alloc, "{d}", .{@divFloor(civil.year, 10)});
    if (std.ascii.eqlIgnoreCase(unit, "century") or std.ascii.eqlIgnoreCase(unit, "centuries")) return try std.fmt.allocPrint(alloc, "{d}", .{@divFloor(civil.year - 1, 100) + 1});
    if (std.ascii.eqlIgnoreCase(unit, "millennium") or std.ascii.eqlIgnoreCase(unit, "millennia") or std.ascii.eqlIgnoreCase(unit, "millenniums")) return try std.fmt.allocPrint(alloc, "{d}", .{@divFloor(civil.year - 1, 1000) + 1});
    if (std.ascii.eqlIgnoreCase(unit, "isoyear")) return try std.fmt.allocPrint(alloc, "{d}", .{rowExpressionIsoWeekYear(day_count)});
    if (std.ascii.eqlIgnoreCase(unit, "quarter") or std.ascii.eqlIgnoreCase(unit, "quarters")) return try std.fmt.allocPrint(alloc, "{d}", .{@divFloor(@as(u16, civil.month) + 2, 3)});
    if (std.ascii.eqlIgnoreCase(unit, "month") or std.ascii.eqlIgnoreCase(unit, "months")) return try std.fmt.allocPrint(alloc, "{d}", .{civil.month});
    if (std.ascii.eqlIgnoreCase(unit, "week") or std.ascii.eqlIgnoreCase(unit, "weeks")) return try std.fmt.allocPrint(alloc, "{d}", .{rowExpressionIsoWeekNumber(day_count)});
    if (std.ascii.eqlIgnoreCase(unit, "day") or std.ascii.eqlIgnoreCase(unit, "days")) return try std.fmt.allocPrint(alloc, "{d}", .{civil.day});
    if (std.ascii.eqlIgnoreCase(unit, "doy")) return try std.fmt.allocPrint(alloc, "{d}", .{rowExpressionDayOfYear(civil)});
    if (std.ascii.eqlIgnoreCase(unit, "dow")) return try std.fmt.allocPrint(alloc, "{d}", .{@mod(day_count + 4, 7)});
    if (std.ascii.eqlIgnoreCase(unit, "isodow")) return try std.fmt.allocPrint(alloc, "{d}", .{@mod(day_count + 3, 7) + 1});
    if (std.ascii.eqlIgnoreCase(unit, "hour") or std.ascii.eqlIgnoreCase(unit, "hours")) return try std.fmt.allocPrint(alloc, "{d}", .{day_ns / row_expression_ns_per_hour});
    if (std.ascii.eqlIgnoreCase(unit, "minute") or std.ascii.eqlIgnoreCase(unit, "minutes")) return try std.fmt.allocPrint(alloc, "{d}", .{(day_ns % row_expression_ns_per_hour) / row_expression_ns_per_minute});
    if (std.ascii.eqlIgnoreCase(unit, "second") or std.ascii.eqlIgnoreCase(unit, "seconds")) {
        const seconds = @as(f64, @floatFromInt(day_ns % row_expression_ns_per_minute)) / @as(f64, @floatFromInt(row_expression_ns_per_second));
        return try std.fmt.allocPrint(alloc, "{d}", .{seconds});
    }
    if (std.ascii.eqlIgnoreCase(unit, "millisecond") or std.ascii.eqlIgnoreCase(unit, "milliseconds")) {
        const milliseconds = @as(f64, @floatFromInt(day_ns % row_expression_ns_per_minute)) / @as(f64, @floatFromInt(row_expression_ns_per_millisecond));
        return try std.fmt.allocPrint(alloc, "{d}", .{milliseconds});
    }
    if (std.ascii.eqlIgnoreCase(unit, "microsecond") or std.ascii.eqlIgnoreCase(unit, "microseconds")) {
        const microseconds = @as(f64, @floatFromInt(day_ns % row_expression_ns_per_minute)) / @as(f64, @floatFromInt(row_expression_ns_per_microsecond));
        return try std.fmt.allocPrint(alloc, "{d}", .{microseconds});
    }
    return error.InvalidColumnValue;
}

fn rowExpressionAddUtcMonthsToTimestampNs(timestamp_ns: u64, months_delta: i64) !u64 {
    const day_count: i64 = @intCast(timestamp_ns / row_expression_ns_per_day);
    const day_ns = timestamp_ns % row_expression_ns_per_day;
    var civil = rowExpressionCivilFromDays(day_count);
    const month_index = civil.year * 12 + @as(i64, civil.month - 1) + months_delta;
    civil.year = @divFloor(month_index, 12);
    civil.month = @intCast(@mod(month_index, 12) + 1);
    const max_day = rowExpressionDaysInMonth(civil.year, civil.month);
    if (civil.day > max_day) civil.day = max_day;
    const out_days = rowExpressionDaysFromCivil(civil.year, civil.month, civil.day);
    if (out_days < 0) return error.InvalidColumnValue;
    const days_ns = std.math.mul(u64, @intCast(out_days), row_expression_ns_per_day) catch return error.InvalidColumnValue;
    return std.math.add(u64, days_ns, day_ns) catch error.InvalidColumnValue;
}

const RowExpressionCivilDate = struct {
    year: i64,
    month: u8,
    day: u8,
};

fn rowExpressionCivilFromDays(days_since_epoch: i64) RowExpressionCivilDate {
    const z = days_since_epoch + 719468;
    const era = @divFloor(z, 146097);
    const doe: u32 = @intCast(z - era * 146097);
    const yoe: u32 = @intCast((doe - doe / 1460 + doe / 36524 - doe / 146096) / 365);
    var year: i64 = @as(i64, yoe) + era * 400;
    const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp = (5 * doy + 2) / 153;
    const day: u8 = @intCast(doy - (153 * mp + 2) / 5 + 1);
    const month: u8 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    if (month <= 2) year += 1;
    return .{ .year = year, .month = month, .day = day };
}

fn rowExpressionDaysFromCivil(year_value: i64, month_value: u8, day_value: u8) i64 {
    var year = year_value;
    const month: i64 = month_value;
    const day: i64 = day_value;
    if (month <= 2) year -= 1;
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const month_prime = month + if (month > 2) @as(i64, -3) else @as(i64, 9);
    const doy = @divFloor(153 * month_prime + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn rowExpressionDayOfYear(civil: RowExpressionCivilDate) u16 {
    const start = rowExpressionDaysFromCivil(civil.year, 1, 1);
    const current = rowExpressionDaysFromCivil(civil.year, civil.month, civil.day);
    return @intCast(current - start + 1);
}

fn rowExpressionIsoWeekYear(day_count: i64) i64 {
    const monday_zero_dow = @mod(day_count + 3, 7);
    const thursday_day = day_count + (3 - monday_zero_dow);
    return rowExpressionCivilFromDays(thursday_day).year;
}

fn rowExpressionIsoWeekNumber(day_count: i64) u8 {
    const year = rowExpressionIsoWeekYear(day_count);
    const jan_4 = rowExpressionDaysFromCivil(year, 1, 4);
    const week_1_monday = jan_4 - @mod(jan_4 + 3, 7);
    return @intCast(@divFloor(day_count - week_1_monday, 7) + 1);
}

fn rowExpressionDaysInMonth(year: i64, month: u8) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (rowExpressionIsLeapYear(year)) 29 else 28,
        else => 0,
    };
}

fn rowExpressionIsLeapYear(year: i64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

fn sqlLikePatternMatches(text: []const u8, pattern: []const u8, case_insensitive: bool) bool {
    return sqlLikePatternMatchesFrom(text, pattern, case_insensitive, 0, 0);
}

fn sqlLikePatternMatchesFrom(
    text: []const u8,
    pattern: []const u8,
    case_insensitive: bool,
    text_index: usize,
    pattern_index: usize,
) bool {
    var ti = text_index;
    var pi = pattern_index;
    while (pi < pattern.len) {
        switch (pattern[pi]) {
            '%' => {
                pi += 1;
                if (pi == pattern.len) return true;
                var next_ti = ti;
                while (next_ti <= text.len) : (next_ti += 1) {
                    if (sqlLikePatternMatchesFrom(text, pattern, case_insensitive, next_ti, pi)) return true;
                }
                return false;
            },
            '_' => {
                if (ti >= text.len) return false;
                ti += 1;
                pi += 1;
            },
            else => {
                if (ti >= text.len) return false;
                if (!sqlLikeBytesEqual(text[ti], pattern[pi], case_insensitive)) return false;
                ti += 1;
                pi += 1;
            },
        }
    }
    return ti == text.len;
}

fn sqlLikeBytesEqual(lhs: u8, rhs: u8, case_insensitive: bool) bool {
    if (!case_insensitive) return lhs == rhs;
    return std.ascii.toLower(lhs) == std.ascii.toLower(rhs);
}

fn replaceTextAlloc(alloc: Allocator, source: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    if (needle.len == 0) return try alloc.dupe(u8, source);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var start: usize = 0;
    while (std.mem.indexOfPos(u8, source, start, needle)) |index| {
        try out.appendSlice(alloc, source[start..index]);
        try out.appendSlice(alloc, replacement);
        start = index + needle.len;
    }
    try out.appendSlice(alloc, source[start..]);
    return try out.toOwnedSlice(alloc);
}

const RegexpReplaceFlags = struct {
    global: bool = false,
};

const RegexpMatchSpan = struct {
    start: usize,
    end: usize,
};

fn regexpReplaceTextAlloc(
    alloc: Allocator,
    source: []const u8,
    pattern: []const u8,
    replacement: []const u8,
    flags: []const u8,
) ![]u8 {
    const parsed_flags = try parseRegexpReplaceFlags(flags);
    if (std.mem.indexOfScalar(u8, replacement, '\\') != null) return error.InvalidColumnValue;

    var compiled = regex_mod.compile(alloc, pattern) catch return error.InvalidColumnValue;
    defer compiled.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var cursor: usize = 0;
    var replaced = false;
    while (cursor <= source.len) {
        const span = regexpFindLeftmostMatch(&compiled, source, cursor) orelse break;
        if (span.end <= span.start) return error.InvalidColumnValue;
        try out.appendSlice(alloc, source[cursor..span.start]);
        try out.appendSlice(alloc, replacement);
        cursor = span.end;
        replaced = true;
        if (!parsed_flags.global) break;
    }
    if (!replaced) return try alloc.dupe(u8, source);
    try out.appendSlice(alloc, source[cursor..]);
    return try out.toOwnedSlice(alloc);
}

fn parseRegexpReplaceFlags(flags: []const u8) !RegexpReplaceFlags {
    var parsed = RegexpReplaceFlags{};
    for (flags) |flag| switch (flag) {
        'g' => parsed.global = true,
        else => return error.InvalidColumnValue,
    };
    return parsed;
}

fn regexpFindLeftmostMatch(compiled: *regex_mod.RegexAutomaton, text: []const u8, start_at: usize) ?RegexpMatchSpan {
    if (compiled.anchored_start and start_at != 0) return null;
    var start = start_at;
    while (start <= text.len) : (start += 1) {
        if (regexpMatchEndFrom(compiled, text, start)) |end| {
            if (compiled.anchored_end and end != text.len) continue;
            return .{ .start = start, .end = end };
        }
    }
    return null;
}

fn regexpMatchEndFrom(compiled: *regex_mod.RegexAutomaton, text: []const u8, start: usize) ?usize {
    const automaton = compiled.automaton();
    var state = automaton.start();
    var latest_match: ?usize = null;
    if (automaton.isMatch(state) and (!compiled.anchored_end or start == text.len)) latest_match = start;
    for (text[start..], 0..) |byte, offset| {
        state = automaton.accept(state, byte);
        if (!automaton.canMatch(state)) break;
        if (automaton.isMatch(state)) {
            const end = start + offset + 1;
            if (!compiled.anchored_end or end == text.len) latest_match = end;
        }
    }
    return latest_match;
}

fn substringTextAlloc(alloc: Allocator, text: []const u8, start_index: i64, length_count: ?i64) ![]u8 {
    if (start_index < 1) return error.InvalidColumnValue;
    if (length_count) |count| if (count < 0) return error.InvalidColumnValue;
    const start_codepoint: usize = @intCast(start_index - 1);
    const end_codepoint: ?usize = if (length_count) |count| start_codepoint + @as(usize, @intCast(count)) else null;
    const start_byte = try utf8ByteOffsetForCodepointIndex(text, start_codepoint);
    const end_byte = if (end_codepoint) |index| try utf8ByteOffsetForCodepointIndex(text, index) else text.len;
    return try alloc.dupe(u8, text[start_byte..end_byte]);
}

fn overlayTextAlloc(alloc: Allocator, text: []const u8, replacement: []const u8, start_index: i64, length_count: ?i64) ![]u8 {
    if (start_index < 1) return error.InvalidColumnValue;
    if (length_count) |count| if (count < 0) return error.InvalidColumnValue;
    try validateUtf8Text(replacement);
    const start_codepoint: usize = @intCast(start_index - 1);
    const replacement_codepoints = std.unicode.utf8CountCodepoints(replacement) catch return error.InvalidColumnValue;
    const replace_count: usize = if (length_count) |count| @intCast(count) else replacement_codepoints;
    const end_codepoint = start_codepoint + replace_count;
    const start_byte = try utf8ByteOffsetForCodepointIndex(text, start_codepoint);
    const end_byte = try utf8ByteOffsetForCodepointIndex(text, end_codepoint);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, text[0..start_byte]);
    try out.appendSlice(alloc, replacement);
    try out.appendSlice(alloc, text[end_byte..]);
    return try out.toOwnedSlice(alloc);
}

fn splitPartTextAlloc(alloc: Allocator, text: []const u8, delimiter: []const u8, field_index: i64) ![]u8 {
    if (field_index == 0) return error.InvalidColumnValue;
    if (delimiter.len == 0) return try alloc.dupe(u8, if (field_index == 1 or field_index == -1) text else "");

    if (field_index > 0) {
        var split = std.mem.splitSequence(u8, text, delimiter);
        var current: i64 = 1;
        while (split.next()) |part| : (current += 1) {
            if (current == field_index) return try alloc.dupe(u8, part);
        }
        return try alloc.dupe(u8, "");
    }

    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var split = std.mem.splitSequence(u8, text, delimiter);
    while (split.next()) |part| try parts.append(alloc, part);
    const from_end: usize = @intCast(-field_index);
    if (from_end == 0 or from_end > parts.items.len) return try alloc.dupe(u8, "");
    return try alloc.dupe(u8, parts.items[parts.items.len - from_end]);
}

fn trimTextAlloc(alloc: Allocator, text: []const u8, trim_set: []const u8, trim_left: bool, trim_right: bool) ![]u8 {
    try validateUtf8Text(trim_set);
    var start: usize = 0;
    var end: usize = text.len;
    if (trim_left) {
        while (start < end) {
            const width = try utf8CodepointWidthAt(text, start);
            if (!try utf8CodepointSetContains(trim_set, text[start .. start + width])) break;
            start += width;
        }
    }
    if (trim_right) {
        while (end > start) {
            const prev = try utf8PreviousCodepointStart(text, start, end);
            if (!try utf8CodepointSetContains(trim_set, text[prev..end])) break;
            end = prev;
        }
    }
    return try alloc.dupe(u8, text[start..end]);
}

fn leftRightTextAlloc(alloc: Allocator, text: []const u8, count: i64, from_left: bool) ![]u8 {
    const total_codepoints = std.unicode.utf8CountCodepoints(text) catch return error.InvalidColumnValue;
    const abs_count: usize = if (count < 0) @intCast(-count) else @intCast(count);
    const slice_start: usize, const slice_end: usize = if (from_left) blk: {
        if (count >= 0) break :blk .{ 0, @min(abs_count, total_codepoints) };
        break :blk .{ 0, total_codepoints - @min(abs_count, total_codepoints) };
    } else blk: {
        if (count >= 0) {
            const kept = @min(abs_count, total_codepoints);
            break :blk .{ total_codepoints - kept, total_codepoints };
        }
        break :blk .{ @min(abs_count, total_codepoints), total_codepoints };
    };
    const start_byte = try utf8ByteOffsetForCodepointIndex(text, slice_start);
    const end_byte = try utf8ByteOffsetForCodepointIndex(text, slice_end);
    return try alloc.dupe(u8, text[start_byte..end_byte]);
}

fn padTextAlloc(alloc: Allocator, text: []const u8, target_count: i64, fill: []const u8, pad_left: bool) ![]u8 {
    try validateUtf8Text(text);
    try validateUtf8Text(fill);
    if (target_count <= 0) return try alloc.dupe(u8, "");
    if (fill.len == 0) return error.InvalidColumnValue;
    const target_codepoints: usize = @intCast(target_count);
    const text_codepoints = std.unicode.utf8CountCodepoints(text) catch return error.InvalidColumnValue;
    if (text_codepoints >= target_codepoints) {
        const end_byte = try utf8ByteOffsetForCodepointIndex(text, target_codepoints);
        return try alloc.dupe(u8, text[0..end_byte]);
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const padding_codepoints = target_codepoints - text_codepoints;
    if (pad_left) {
        try appendPadCodepoints(alloc, &out, fill, padding_codepoints);
        try out.appendSlice(alloc, text);
    } else {
        try out.appendSlice(alloc, text);
        try appendPadCodepoints(alloc, &out, fill, padding_codepoints);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendPadCodepoints(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), fill: []const u8, count: usize) !void {
    var appended: usize = 0;
    while (appended < count) {
        var index: usize = 0;
        while (index < fill.len and appended < count) : (appended += 1) {
            const width = try utf8CodepointWidthAt(fill, index);
            try out.appendSlice(alloc, fill[index .. index + width]);
            index += width;
        }
    }
}

fn repeatTextAlloc(alloc: Allocator, text: []const u8, count: i64) ![]u8 {
    try validateUtf8Text(text);
    if (count < 0) return error.InvalidColumnValue;
    const repeat_count: usize = @intCast(count);
    const total_len = std.math.mul(usize, text.len, repeat_count) catch return error.InvalidColumnValue;
    var out = try std.ArrayListUnmanaged(u8).initCapacity(alloc, total_len);
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < repeat_count) : (i += 1) try out.appendSlice(alloc, text);
    return try out.toOwnedSlice(alloc);
}

fn reverseTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    try validateUtf8Text(text);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var end = text.len;
    while (end > 0) {
        const start = try utf8PreviousCodepointStart(text, 0, end);
        try out.appendSlice(alloc, text[start..end]);
        end = start;
    }
    return try out.toOwnedSlice(alloc);
}

fn initcapTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    try validateUtf8Text(text);
    const out = try alloc.dupe(u8, text);
    var at_word_start = true;
    for (out) |*byte| {
        if (std.ascii.isAlphanumeric(byte.*)) {
            byte.* = if (at_word_start) std.ascii.toUpper(byte.*) else std.ascii.toLower(byte.*);
            at_word_start = false;
        } else {
            at_word_start = true;
        }
    }
    return out;
}

fn translateTextAlloc(alloc: Allocator, text: []const u8, from_set: []const u8, to_set: []const u8) ![]u8 {
    try validateUtf8Text(text);
    try validateUtf8Text(from_set);
    try validateUtf8Text(to_set);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var index: usize = 0;
    while (index < text.len) {
        const width = try utf8CodepointWidthAt(text, index);
        const codepoint = text[index .. index + width];
        if (try translateReplacementCodepoint(from_set, to_set, codepoint)) |replacement| {
            try out.appendSlice(alloc, replacement);
        } else if (!try utf8CodepointSetContains(from_set, codepoint)) {
            try out.appendSlice(alloc, codepoint);
        }
        index += width;
    }
    return try out.toOwnedSlice(alloc);
}

fn translateReplacementCodepoint(from_set: []const u8, to_set: []const u8, needle: []const u8) !?[]const u8 {
    var from_index: usize = 0;
    var ordinal: usize = 0;
    while (from_index < from_set.len) : (ordinal += 1) {
        const from_width = try utf8CodepointWidthAt(from_set, from_index);
        if (std.mem.eql(u8, from_set[from_index .. from_index + from_width], needle)) {
            var to_index: usize = 0;
            var to_ordinal: usize = 0;
            while (to_index < to_set.len) : (to_ordinal += 1) {
                const to_width = try utf8CodepointWidthAt(to_set, to_index);
                if (to_ordinal == ordinal) return to_set[to_index .. to_index + to_width];
                to_index += to_width;
            }
            return "";
        }
        from_index += from_width;
    }
    return null;
}

fn validateUtf8Text(text: []const u8) !void {
    var index: usize = 0;
    while (index < text.len) index += try utf8CodepointWidthAt(text, index);
}

fn firstUtf8Codepoint(text: []const u8) !u21 {
    const width = try utf8CodepointWidthAt(text, 0);
    return std.unicode.utf8Decode(text[0..width]) catch return error.InvalidColumnValue;
}

fn codepointTextAlloc(alloc: Allocator, codepoint: i64) ![]u8 {
    if (codepoint <= 0 or codepoint > 0x10ffff) return error.InvalidColumnValue;
    if (codepoint >= 0xd800 and codepoint <= 0xdfff) return error.InvalidColumnValue;
    var buf: [4]u8 = undefined;
    const len = std.unicode.utf8Encode(@intCast(codepoint), &buf) catch return error.InvalidColumnValue;
    return try alloc.dupe(u8, buf[0..len]);
}

fn utf8CodepointWidthAt(text: []const u8, index: usize) !usize {
    if (index >= text.len) return error.InvalidColumnValue;
    const width = std.unicode.utf8ByteSequenceLength(text[index]) catch return error.InvalidColumnValue;
    if (index + width > text.len) return error.InvalidColumnValue;
    return width;
}

fn utf8PreviousCodepointStart(text: []const u8, start: usize, end: usize) !usize {
    if (end <= start or end > text.len) return error.InvalidColumnValue;
    var index = end;
    while (index > start) {
        index -= 1;
        if ((text[index] & 0b1100_0000) != 0b1000_0000) {
            const width = try utf8CodepointWidthAt(text, index);
            if (index + width != end) return error.InvalidColumnValue;
            return index;
        }
    }
    return error.InvalidColumnValue;
}

fn utf8CodepointSetContains(set: []const u8, needle: []const u8) !bool {
    var index: usize = 0;
    while (index < set.len) {
        const width = try utf8CodepointWidthAt(set, index);
        if (std.mem.eql(u8, set[index .. index + width], needle)) return true;
        index += width;
    }
    return false;
}

fn strposTextCodepointPosition(text: []const u8, needle: []const u8) !usize {
    if (needle.len == 0) return 1;
    const byte_index = std.mem.indexOf(u8, text, needle) orelse return 0;
    const codepoints_before = std.unicode.utf8CountCodepoints(text[0..byte_index]) catch return error.InvalidColumnValue;
    return codepoints_before + 1;
}

fn utf8ByteOffsetForCodepointIndex(text: []const u8, codepoint_index: usize) !usize {
    var byte_index: usize = 0;
    var seen: usize = 0;
    while (byte_index < text.len and seen < codepoint_index) : (seen += 1) {
        const width = std.unicode.utf8ByteSequenceLength(text[byte_index]) catch return error.InvalidColumnValue;
        if (byte_index + width > text.len) return error.InvalidColumnValue;
        byte_index += width;
    }
    return byte_index;
}

fn cellJsonValueAlloc(alloc: Allocator, cell: relational_row_codec.Cell) ![]u8 {
    return switch (cell.value) {
        .bytes_val => |bytes| if (cell.is_json)
            try alloc.dupe(u8, bytes)
        else
            try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .string = bytes }, .{}),
        .bool_val => |value| try alloc.dupe(u8, if (value) "true" else "false"),
        .u64_val => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
        .i64_val => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
        .f64_val => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
        .geo_point => return error.InvalidColumnValue,
    };
}

fn md5HexTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    const digest = std.crypto.hash.Md5.hashResult(text);
    const out = try alloc.alloc(u8, digest.len * 2);
    for (digest, 0..) |byte, i| {
        out[i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn soundexTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    var out = try alloc.dupe(u8, "?000");
    errdefer alloc.free(out);

    var previous_code: u8 = 0;
    var output_index: usize = 1;
    var found_first = false;
    for (text) |byte| {
        if (!std.ascii.isAlphabetic(byte)) {
            previous_code = 0;
            continue;
        }
        const upper = std.ascii.toUpper(byte);
        const code = soundexCode(upper);
        if (!found_first) {
            out[0] = upper;
            previous_code = code;
            found_first = true;
            continue;
        }
        if (code == 0) {
            if (upper != 'H' and upper != 'W') previous_code = 0;
            continue;
        }
        if (code == previous_code) continue;
        if (output_index < out.len) {
            out[output_index] = '0' + code;
            output_index += 1;
        }
        previous_code = code;
        if (output_index == out.len) break;
    }

    return out;
}

fn soundexCode(upper: u8) u8 {
    return switch (upper) {
        'B', 'F', 'P', 'V' => 1,
        'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z' => 2,
        'D', 'T' => 3,
        'L' => 4,
        'M', 'N' => 5,
        'R' => 6,
        else => 0,
    };
}

fn cellEqualsJsonLiteral(alloc: Allocator, cell: relational_row_codec.Cell, value_json: []const u8) !bool {
    return try cellEqualsJsonLiteralWithCollation(alloc, cell, value_json, null);
}

fn cellEqualsJsonLiteralWithCollation(alloc: Allocator, cell: relational_row_codec.Cell, value_json: []const u8, collation: ?[]const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    switch (cell.value) {
        .bytes_val => |bytes| {
            if (parsed.value != .string) return false;
            if (collation) |name| {
                if (relational_collation.isCaseInsensitive(name)) return std.ascii.eqlIgnoreCase(bytes, parsed.value.string);
            }
            return std.mem.eql(u8, bytes, parsed.value.string);
        },
        .bool_val => |value| return parsed.value == .bool and value == parsed.value.bool,
        .u64_val => |value| switch (parsed.value) {
            .integer => |parsed_int| return parsed_int >= 0 and value == @as(u64, @intCast(parsed_int)),
            else => return false,
        },
        .i64_val => |value| switch (parsed.value) {
            .integer => |parsed_int| return value == parsed_int,
            else => return false,
        },
        .f64_val => |value| switch (parsed.value) {
            .integer => |parsed_int| return value == @as(f64, @floatFromInt(parsed_int)),
            .float => |parsed_float| return value == parsed_float,
            else => return false,
        },
        .geo_point => return false,
    }
}

fn uniqueConstraintKeysTupleValueAlloc(
    alloc: Allocator,
    row_value: []const u8,
    columns: []const []const u8,
    expressions: []const schema_mod.UniqueExpression,
    include_nulls: bool,
    relational_columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueWithColumnsAlloc(alloc, row_value, column_path, relational_columns)) orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    for (expressions) |expression| {
        const component = (try uniqueConstraintExpressionValueWithColumnsAlloc(alloc, row_value, expression, relational_columns)) orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    return try out.toOwnedSlice(alloc);
}

fn uniqueConstraintKeysTupleValueFromCellsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    columns: []const []const u8,
    expressions: []const schema_mod.UniqueExpression,
    include_nulls: bool,
    relational_columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueFromCellsWithColumnsAlloc(alloc, cells, column_path, relational_columns)) orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    for (expressions) |expression| {
        const component = (try uniqueConstraintExpressionValueFromCellsWithColumnsAlloc(alloc, row_value, cells, expression, relational_columns)) orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    return try out.toOwnedSlice(alloc);
}

fn uniqueConstraintKeysTupleValueWithExpressionValuesAlloc(
    alloc: Allocator,
    row_value: []const u8,
    columns: []const []const u8,
    expressions: []const schema_mod.UniqueExpression,
    include_nulls: bool,
    relational_columns: []const schema_mod.RelationalColumn,
    expression_value_jsons: []const ?[]const u8,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueWithColumnsAlloc(alloc, row_value, column_path, relational_columns)) orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    for (expressions, 0..) |expression, index| {
        const component = if (expression_value_jsons[index]) |value_json|
            try uniqueConstraintExpressionJsonValueWithColumnsAlloc(alloc, value_json, expression, relational_columns)
        else
            try uniqueConstraintExpressionValueWithColumnsAlloc(alloc, row_value, expression, relational_columns);
        const owned_component = component orelse {
            if (!include_nulls) {
                out.deinit(alloc);
                return null;
            }
            try internal_keys.appendEncodedComponent(&out, alloc, typedJsonNullValue());
            continue;
        };
        defer alloc.free(owned_component);
        try internal_keys.appendEncodedComponent(&out, alloc, owned_component);
    }
    return try out.toOwnedSlice(alloc);
}

fn typedJsonNullValue() []const u8 {
    return &[_]u8{0xff};
}

fn requiredConstraintColumnsTupleValueAlloc(alloc: Allocator, row_value: []const u8, columns: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueAlloc(alloc, row_value, column_path)) orelse {
            return error.UniqueConstraintViolation;
        };
        defer alloc.free(component);
        try internal_keys.appendEncodedComponent(&out, alloc, component);
    }
    return try out.toOwnedSlice(alloc);
}

fn requiredConstraintColumnsTupleValueFromCellsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    columns: []const []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (columns) |column_path| {
        const component = (try uniqueConstraintColumnValueFromCellsWithColumnsAlloc(alloc, cells, column_path, &.{})) orelse {
            return error.UniqueConstraintViolation;
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
        .i64_val => blk: {
            if (payload.len != 8) return error.InvalidColumnValue;
            const sortable_bits = std.mem.readInt(u64, payload[0..8], .big);
            const value: i64 = @bitCast(sortable_bits ^ 0x8000_0000_0000_0000);
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
        @intFromEnum(typed_dv.ValueType.i64_val) => .i64_val,
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
    return try uniqueConstraintColumnValueWithColumnsAlloc(alloc, row_value, column_path, &.{});
}

fn uniqueConstraintColumnValueWithColumnsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    column_path: []const u8,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    const column = findRelationalColumn(columns, column_path);
    const collation = if (column) |resolved| resolved.collation else null;
    return try uniqueConstraintColumnValueWithCollationAlloc(alloc, row_value, column_path, collation);
}

fn uniqueConstraintColumnValueFromCellsWithColumnsAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    column_path: []const u8,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    const column = findRelationalColumn(columns, column_path);
    const collation = if (column) |resolved| resolved.collation else null;
    const cell = findCellByColumnPathInCells(cells, column_path, columns) orelse return null;
    if (cell.is_json) return error.InvalidColumnValue;
    return try uniqueConstraintCellValueWithCollationAlloc(alloc, cell, collation);
}

fn uniqueConstraintColumnValueFromCellsWithCollationAlloc(
    alloc: Allocator,
    cells: []const relational_row_codec.Cell,
    column_path: []const u8,
    collation: ?[]const u8,
) !?[]u8 {
    const cell = findCellInRow(cells, column_path) orelse return null;
    if (cell.is_json) return error.InvalidColumnValue;
    return try uniqueConstraintCellValueWithCollationAlloc(alloc, cell, collation);
}

fn uniqueConstraintColumnValueWithCollationAlloc(
    alloc: Allocator,
    row_value: []const u8,
    column_path: []const u8,
    collation: ?[]const u8,
) !?[]u8 {
    const cell = (try relational_row_codec.findCellByPath(row_value, column_path)) orelse return null;
    if (cell.is_json) return error.InvalidColumnValue;
    return try uniqueConstraintCellValueWithCollationAlloc(alloc, cell, collation);
}

fn uniqueConstraintExpressionValueAlloc(alloc: Allocator, row_value: []const u8, expression: schema_mod.UniqueExpression) !?[]u8 {
    return try uniqueConstraintExpressionValueWithColumnsAlloc(alloc, row_value, expression, &.{});
}

fn uniqueConstraintExpressionValueWithColumnsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    expression: schema_mod.UniqueExpression,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    return switch (expression.op) {
        .lower, .upper, .md5 => blk: {
            const cell = (try relational_row_codec.findCellByPath(row_value, expression.field)) orelse return null;
            if (cell.is_json) return error.InvalidColumnValue;
            const bytes = switch (cell.value) {
                .bytes_val => |value| value,
                else => return error.InvalidColumnValue,
            };
            switch (expression.op) {
                .lower, .upper => {
                    const folded = try alloc.alloc(u8, bytes.len + 1);
                    folded[0] = @intFromEnum(typed_dv.ValueType.bytes_val);
                    for (bytes, 0..) |ch, i| {
                        folded[i + 1] = switch (expression.op) {
                            .lower => std.ascii.toLower(ch),
                            .upper => std.ascii.toUpper(ch),
                            .md5 => unreachable,
                            .expression => unreachable,
                        };
                    }
                    break :blk folded;
                },
                .md5 => {
                    const digest = std.crypto.hash.Md5.hashResult(bytes);
                    const hashed = try alloc.alloc(u8, 33);
                    hashed[0] = @intFromEnum(typed_dv.ValueType.bytes_val);
                    for (digest, 0..) |byte, i| {
                        hashed[1 + i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
                        hashed[1 + i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
                    }
                    break :blk hashed;
                },
                .expression => unreachable,
            }
        },
        .expression => blk: {
            const row_expression = expression.expression orelse return error.InvalidColumnValue;
            const value_json = try rowExpressionValueJsonAlloc(alloc, row_value, row_expression);
            defer alloc.free(value_json);
            const collation = rowExpressionDirectFieldCollation(columns, row_expression);
            break :blk try uniqueConstraintJsonValueWithCollationAlloc(alloc, value_json, collation);
        },
    };
}

fn uniqueConstraintExpressionValueFromCellsWithColumnsAlloc(
    alloc: Allocator,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    expression: schema_mod.UniqueExpression,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    return switch (expression.op) {
        .lower, .upper, .md5 => blk: {
            const cell = findCellByColumnPathInCells(cells, expression.field, columns) orelse return null;
            if (cell.is_json) return error.InvalidColumnValue;
            const bytes = switch (cell.value) {
                .bytes_val => |value| value,
                else => return error.InvalidColumnValue,
            };
            switch (expression.op) {
                .lower, .upper => {
                    const folded = try alloc.alloc(u8, bytes.len + 1);
                    folded[0] = @intFromEnum(typed_dv.ValueType.bytes_val);
                    for (bytes, 0..) |ch, i| {
                        folded[i + 1] = switch (expression.op) {
                            .lower => std.ascii.toLower(ch),
                            .upper => std.ascii.toUpper(ch),
                            .md5 => unreachable,
                            .expression => unreachable,
                        };
                    }
                    break :blk folded;
                },
                .md5 => {
                    const digest = std.crypto.hash.Md5.hashResult(bytes);
                    const hashed = try alloc.alloc(u8, 33);
                    hashed[0] = @intFromEnum(typed_dv.ValueType.bytes_val);
                    for (digest, 0..) |byte, i| {
                        hashed[1 + i * 2] = std.fmt.digitToChar(byte >> 4, .lower);
                        hashed[1 + i * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
                    }
                    break :blk hashed;
                },
                .expression => unreachable,
            }
        },
        .expression => try uniqueConstraintExpressionValueWithColumnsAlloc(alloc, row_value, expression, columns),
    };
}

fn uniqueConstraintJsonValueAlloc(alloc: Allocator, value_json: []const u8) !?[]u8 {
    return try uniqueConstraintJsonValueWithCollationAlloc(alloc, value_json, null);
}

fn uniqueConstraintExpressionJsonValueWithColumnsAlloc(
    alloc: Allocator,
    value_json: []const u8,
    expression: schema_mod.UniqueExpression,
    columns: []const schema_mod.RelationalColumn,
) !?[]u8 {
    const collation = switch (expression.op) {
        .lower, .upper, .md5 => null,
        .expression => if (expression.expression) |row_expression| rowExpressionDirectFieldCollation(columns, row_expression) else null,
    };
    return try uniqueConstraintJsonValueWithCollationAlloc(alloc, value_json, collation);
}

fn uniqueConstraintJsonValueWithCollationAlloc(alloc: Allocator, value_json: []const u8, collation: ?[]const u8) !?[]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidColumnValue;
    defer parsed.deinit();
    const cell: relational_row_codec.Cell = switch (parsed.value) {
        .null => return null,
        .string => |value| .{
            .path = "",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = value },
        },
        .bool => |value| .{
            .path = "",
            .value_type = .bool_val,
            .value = .{ .bool_val = value },
        },
        .integer => |value| .{
            .path = "",
            .value_type = .u64_val,
            .value = .{ .u64_val = @bitCast(value) },
        },
        .float => |value| .{
            .path = "",
            .value_type = .f64_val,
            .value = .{ .f64_val = value },
        },
        .number_string => |value| blk: {
            if (std.fmt.parseInt(i64, value, 10)) |integer| {
                break :blk .{
                    .path = "",
                    .value_type = .u64_val,
                    .value = .{ .u64_val = @bitCast(integer) },
                };
            } else |_| {
                break :blk .{
                    .path = "",
                    .value_type = .f64_val,
                    .value = .{ .f64_val = std.fmt.parseFloat(f64, value) catch return error.InvalidColumnValue },
                };
            }
        },
        else => return error.InvalidColumnValue,
    };
    return try uniqueConstraintCellValueWithCollationAlloc(alloc, cell, collation);
}

fn rowExpressionDirectFieldCollation(
    columns: []const schema_mod.RelationalColumn,
    expression: schema_mod.RelationalRowsExpression,
) ?[]const u8 {
    if (expression.kind != .field or expression.field_source != .row) return null;
    return relationalColumnCollation(columns, expression.field);
}

fn uniqueConstraintCellValueAlloc(alloc: Allocator, cell: relational_row_codec.Cell) ![]u8 {
    return try uniqueConstraintCellValueWithCollationAlloc(alloc, cell, null);
}

fn uniqueConstraintCellValueWithCollationAlloc(
    alloc: Allocator,
    cell: relational_row_codec.Cell,
    collation: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, @intFromEnum(cell.value_type));
    switch (cell.value) {
        .u64_val => |value| {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, value, .big);
            try out.appendSlice(alloc, &buf);
        },
        .i64_val => |value| {
            var buf: [8]u8 = undefined;
            const sortable_bits: u64 = @as(u64, @bitCast(value)) ^ 0x8000_0000_0000_0000;
            std.mem.writeInt(u64, &buf, sortable_bits, .big);
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
        .bytes_val => |value| {
            if (collation) |name| {
                if (relational_collation.isCaseInsensitive(name)) {
                    for (value) |ch| try out.append(alloc, std.ascii.toLower(ch));
                    return try out.toOwnedSlice(alloc);
                }
            }
            try out.appendSlice(alloc, value);
        },
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
    try appendUpsertInternal(alloc, store, writes, deletes, owned_keys, owned_values, doc_key, row_value, ColumnIndexPolicy.empty());
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
    try appendDeleteInternal(alloc, store, deletes, owned_keys, doc_key, ColumnIndexPolicy.empty());
}

pub fn appendDeleteWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendDeleteInternal(alloc, store, deletes, owned_keys, doc_key, column_index_policy);
}

fn appendDeleteWithColumnIndexPolicyAndOldRowAndCells(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    old_cells: ?[]const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendDeleteInternalWithOldRowAndCells(alloc, deletes, owned_keys, doc_key, old_row_value, old_cells, column_index_policy);
}

pub fn appendUpsertOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendUpsertInternal(alloc, store, writes, deletes, null, null, doc_key, row_value, ColumnIndexPolicy.empty());
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

fn appendUpsertWithColumnIndexPolicyAndOldRow(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    try appendUpsertInternalWithOldRow(alloc, writes, deletes, owned_keys, owned_values, doc_key, old_row_value, row_value, column_index_policy, 0);
}

fn appendUpsertWithColumnIndexPolicyAndOldRowAndCells(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    old_cells: ?[]const relational_row_codec.Cell,
    row_value: []const u8,
    new_cells: ?[]const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
    row_generation: u64,
) !void {
    try appendUpsertInternalWithOldRowAndCells(alloc, writes, deletes, owned_keys, owned_values, doc_key, old_row_value, old_cells, row_value, new_cells, column_index_policy, row_generation);
}

pub fn appendDeleteOwnedBatch(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    doc_key: []const u8,
) !void {
    try appendDeleteInternal(alloc, store, deletes, null, doc_key, ColumnIndexPolicy.empty());
}

pub fn appendColumnIndexWritesForRow(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
) !void {
    try appendColumnIndexWritesForRowWithColumnIndexPolicy(alloc, writes, owned_keys, owned_values, doc_key, row_value, ColumnIndexPolicy.empty());
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
    if (!column_index_policy.mayIndexAnyRow()) return;
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        if (!(try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, row_value))) continue;
        try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, cell);
    }
}

pub fn appendColumnBackedIndexWritesForRowWithColumnIndexPolicy(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    if (!column_index_policy.mayIndexAnyRow()) return;
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);
    for (row.cells) |cell| {
        if (!(try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, row_value))) continue;
        try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, cell);
    }
    try appendOrderedTupleIndexWritesForRowWithColumnIndexPolicy(
        alloc,
        writes,
        owned_keys,
        owned_values,
        doc_key,
        row_value,
        row.cells,
        column_index_policy,
        0,
    );
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

pub fn scanRowsSpanAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    start_doc_key: []const u8,
    end_doc_key: []const u8,
) ![]OwnedRow {
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, start_doc_key);
    defer alloc.free(lower);
    const upper = if (end_doc_key.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, end_doc_key) else null;
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

const OwnedRowPage = struct {
    rows: []OwnedRow = &.{},
    next_start_doc_key: []u8 = "",

    fn deinit(self: *@This(), alloc: Allocator) void {
        freeRows(alloc, self.rows);
        if (self.next_start_doc_key.len > 0) alloc.free(self.next_start_doc_key);
        self.* = undefined;
    }
};

fn scanRowsSpanPageAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    start_doc_key: []const u8,
    end_doc_key: []const u8,
    max_rows: usize,
) !OwnedRowPage {
    if (max_rows == 0) return error.InvalidSecondaryIndexRebuildRequest;
    const lower = try internal_keys.documentRangeLowerAlloc(alloc, start_doc_key);
    defer alloc.free(lower);
    const upper = if (end_doc_key.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, end_doc_key) else null;
    defer if (upper) |buf| alloc.free(buf);

    var out = std.ArrayListUnmanaged(OwnedRow).empty;
    errdefer {
        for (out.items) |*row| row.deinit(alloc);
        out.deinit(alloc);
    }

    const ScanContext = struct {
        alloc: Allocator,
        max_rows: usize,
        rows: *std.ArrayListUnmanaged(OwnedRow),
        next_start_doc_key: []u8 = "",

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidSecondaryIndexRebuildRequest));
            if (!internal_keys.isRelationalRowKey(key)) return .@"continue";
            const doc_key = (try internal_keys.decodeRelationalRowKeyAlloc(self.alloc, key)) orelse return .@"continue";
            errdefer self.alloc.free(doc_key);
            if (self.rows.items.len >= self.max_rows) {
                self.next_start_doc_key = doc_key;
                return .stop;
            }
            const row_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(row_value);
            try self.rows.append(self.alloc, .{
                .doc_key = doc_key,
                .row_value = row_value,
            });
            return .@"continue";
        }
    };

    var scan_context = ScanContext{
        .alloc = alloc,
        .max_rows = max_rows,
        .rows = &out,
    };
    errdefer if (scan_context.next_start_doc_key.len > 0) alloc.free(scan_context.next_start_doc_key);
    try store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &scan_context, ScanContext.scanEntry);

    const rows = try out.toOwnedSlice(alloc);
    errdefer freeRows(alloc, rows);
    const next = scan_context.next_start_doc_key;
    scan_context.next_start_doc_key = "";
    return .{ .rows = rows, .next_start_doc_key = next };
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

pub fn arrayElementIndexKeyForValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

pub fn arrayValueIndexKeyForValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

pub fn jsonValueIndexKeyForValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

pub fn freeDocKeys(alloc: Allocator, doc_keys: [][]u8) void {
    for (doc_keys) |doc_key| alloc.free(doc_key);
    alloc.free(doc_keys);
}

pub const OrderedTupleDocKeyScanStats = struct {
    iterator_seeks: u64 = 0,
    index_entries_scanned: u64 = 0,
    candidate_rows: u64 = 0,
};

pub const OrderedTupleDocKeyScanCallback = *const fn (
    ctx: ?*anyopaque,
    entry_key: []const u8,
    doc_key: []const u8,
    payload: []const u8,
) anyerror!docstore_mod.DocStore.ScanAction;

const OrderedTupleDocKeyScanContext = struct {
    alloc: Allocator,
    lower_doc_bound: []const u8,
    upper_doc_bound: ?[]const u8,
    stats: ?*OrderedTupleDocKeyScanStats,
    user_ctx: ?*anyopaque,
    callback: OrderedTupleDocKeyScanCallback,

    fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
        const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        if (self.stats) |stats| stats.index_entries_scanned += 1;
        const doc_key = (try internal_keys.decodeRelationalOrderedTupleIndexDocKeyAlloc(self.alloc, key)) orelse return .@"continue";
        defer self.alloc.free(doc_key);
        if (!(try docKeyFallsInRange(self.alloc, doc_key, self.lower_doc_bound, self.upper_doc_bound))) return .@"continue";
        if (self.stats) |stats| stats.candidate_rows += 1;
        return try self.callback(self.user_ctx, key, doc_key, value);
    }
};

fn scanOrderedTupleDocKeysInKeyRangeWithContext(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower: []const u8,
    upper: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    stats: ?*OrderedTupleDocKeyScanStats,
    ctx: ?*anyopaque,
    callback: OrderedTupleDocKeyScanCallback,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    var scan_ctx = OrderedTupleDocKeyScanContext{
        .alloc = alloc,
        .lower_doc_bound = lower_doc_bound,
        .upper_doc_bound = upper_doc_bound,
        .stats = stats,
        .user_ctx = ctx,
        .callback = callback,
    };
    if (stats) |out| out.iterator_seeks += 1;
    try store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &scan_ctx, OrderedTupleDocKeyScanContext.run);
}

pub fn scanOrderedTupleDocKeysWithContext(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_id: []const u8,
    encoded_tuple_prefix: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    stats: ?*OrderedTupleDocKeyScanStats,
    ctx: ?*anyopaque,
    callback: OrderedTupleDocKeyScanCallback,
) !void {
    const lower = try internal_keys.relationalOrderedTupleIndexTupleRangeLowerAlloc(alloc, index_id, encoded_tuple_prefix);
    defer alloc.free(lower);
    const upper = try internal_keys.relationalOrderedTupleIndexTupleRangeUpperAlloc(alloc, index_id, encoded_tuple_prefix);
    defer if (upper) |buf| alloc.free(buf);
    try scanOrderedTupleDocKeysInKeyRangeWithContext(alloc, store, lower, upper, lower_doc_key, upper_doc_key, stats, ctx, callback);
}

pub fn scanOrderedTupleDocKeysInTupleRangeWithContext(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_id: []const u8,
    lower_encoded_tuple: []const u8,
    upper_encoded_tuple_exclusive: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    stats: ?*OrderedTupleDocKeyScanStats,
    ctx: ?*anyopaque,
    callback: OrderedTupleDocKeyScanCallback,
) !void {
    const lower = try internal_keys.relationalOrderedTupleIndexTupleRangeLowerAlloc(alloc, index_id, lower_encoded_tuple);
    defer alloc.free(lower);
    const upper = if (upper_encoded_tuple_exclusive) |tuple|
        try internal_keys.relationalOrderedTupleIndexTupleRangeLowerAlloc(alloc, index_id, tuple)
    else
        try internal_keys.relationalOrderedTupleIndexTupleRangeUpperAlloc(alloc, index_id, "");
    defer if (upper) |buf| alloc.free(buf);
    try scanOrderedTupleDocKeysInKeyRangeWithContext(alloc, store, lower, upper, lower_doc_key, upper_doc_key, stats, ctx, callback);
}

const OrderedTupleDocKeyMaterializer = struct {
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]u8),

    fn append(ctx: ?*anyopaque, _: []const u8, doc_key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
        const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        try self.out.append(self.alloc, try self.alloc.dupe(u8, doc_key));
        return .@"continue";
    }
};

pub fn scanOrderedTupleDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_id: []const u8,
    encoded_tuple_prefix: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }
    var materializer = OrderedTupleDocKeyMaterializer{ .alloc = alloc, .out = &out };
    try scanOrderedTupleDocKeysWithContext(alloc, store, index_id, encoded_tuple_prefix, lower_doc_key, upper_doc_key, null, &materializer, OrderedTupleDocKeyMaterializer.append);

    return try out.toOwnedSlice(alloc);
}

pub fn scanOrderedTupleDocKeysInTupleRangeAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_id: []const u8,
    lower_encoded_tuple: []const u8,
    upper_encoded_tuple_exclusive: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }
    var materializer = OrderedTupleDocKeyMaterializer{ .alloc = alloc, .out = &out };
    try scanOrderedTupleDocKeysInTupleRangeWithContext(alloc, store, index_id, lower_encoded_tuple, upper_encoded_tuple_exclusive, lower_doc_key, upper_doc_key, null, &materializer, OrderedTupleDocKeyMaterializer.append);

    return try out.toOwnedSlice(alloc);
}

pub fn scanArrayElementDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    element_key: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalArrayElementIndexPrefixAlloc(alloc, column_path, element_key);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalArrayElementIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!std.mem.eql(u8, decoded.element_key, element_key)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowHasArrayElementKey(alloc, store, decoded.doc_key, column_path, element_key)) continue;
        try out.append(alloc, try alloc.dupe(u8, decoded.doc_key));
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanArrayElementTermRangeDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
    collation: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalArrayElementIndexColumnPrefixAlloc(alloc, column_path);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalArrayElementIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!try arrayElementIndexKeyStringInRange(alloc, decoded.element_key, min, max, inclusive_min, inclusive_max, collation)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowHasArrayElementTermRange(alloc, store, decoded.doc_key, column_path, min, max, inclusive_min, inclusive_max, collation)) continue;
        try appendUniqueDocKeyAlloc(alloc, &out, decoded.doc_key);
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanArrayValueDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    array_key: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalArrayValueIndexPrefixAlloc(alloc, column_path, array_key);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalArrayValueIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!std.mem.eql(u8, decoded.array_key, array_key)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowArrayValueEquals(alloc, store, decoded.doc_key, column_path, array_key)) continue;
        try out.append(alloc, try alloc.dupe(u8, decoded.doc_key));
    }

    return try out.toOwnedSlice(alloc);
}

fn appendUniqueDocKeyAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged([]u8), doc_key: []const u8) !void {
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, doc_key)) return;
    }
    try out.append(alloc, try alloc.dupe(u8, doc_key));
}

fn currentRowHasArrayElementKey(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    element_key: []const u8,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    if (cell.value_type != .bytes_val or !cell.is_json) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    for (parsed.value.array.items) |item| {
        const item_key = try arrayElementIndexKeyForValueAlloc(alloc, item);
        defer alloc.free(item_key);
        if (std.mem.eql(u8, item_key, element_key)) return true;
    }
    return false;
}

fn currentRowHasArrayElementTermRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
    collation: ?[]const u8,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    const value = OwnedColumnValue{
        .doc_key = @constCast(doc_key),
        .value_type = cell.value_type,
        .is_json = cell.is_json,
        .value = cell.value,
    };
    return try arrayColumnValueContainsTermRange(alloc, value, min, max, inclusive_min, inclusive_max, collation);
}

pub fn arrayColumnValueContainsTermRange(
    alloc: Allocator,
    value: OwnedColumnValue,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
    collation: ?[]const u8,
) !bool {
    if (value.value_type != .bytes_val or !value.is_json) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value.value.bytes_val, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    for (parsed.value.array.items) |item| {
        if (item != .string) continue;
        if (stringInTermRange(item.string, min, max, inclusive_min, inclusive_max, collation)) return true;
    }
    return false;
}

fn arrayElementIndexKeyStringInRange(
    alloc: Allocator,
    element_key: []const u8,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
    collation: ?[]const u8,
) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, element_key, .{});
    defer parsed.deinit();
    if (parsed.value != .string) return false;
    return stringInTermRange(parsed.value.string, min, max, inclusive_min, inclusive_max, collation);
}

fn stringInTermRange(
    value: []const u8,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
    collation: ?[]const u8,
) bool {
    const above_min = if (min) |lower| blk: {
        const comparison = relational_collation.compareTextForScalar(value, lower, collation);
        break :blk comparison == .gt or (inclusive_min and comparison == .eq);
    } else true;
    const below_max = if (max) |upper| blk: {
        const comparison = relational_collation.compareTextForScalar(value, upper, collation);
        break :blk comparison == .lt or (inclusive_max and comparison == .eq);
    } else true;
    return above_min and below_max;
}

fn currentRowArrayValueEquals(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    array_key: []const u8,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    if (cell.value_type != .bytes_val or !cell.is_json) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return false;
    const current_key = try arrayValueIndexKeyForValueAlloc(alloc, parsed.value);
    defer alloc.free(current_key);
    return std.mem.eql(u8, current_key, array_key);
}

pub fn jsonContainsHasIndexableLeaf(value: std.json.Value) bool {
    switch (value) {
        .null, .bool, .integer, .float, .number_string, .string => return true,
        .array => |array| {
            for (array.items) |item| {
                if (jsonContainsHasIndexableLeaf(item)) return true;
            }
            return false;
        },
        .object => |object| {
            for (object.values()) |item| {
                if (jsonContainsHasIndexableLeaf(item)) return true;
            }
            return false;
        },
    }
}

pub fn scanJsonContainmentDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    wanted: std.json.Value,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    var leaves = std.ArrayListUnmanaged(JsonValueIndexEntry).empty;
    defer {
        for (leaves.items) |*entry| entry.deinit(alloc);
        leaves.deinit(alloc);
    }
    try collectJsonValueIndexEntriesAlloc(alloc, "", wanted, &leaves);
    if (leaves.items.len == 0) return try alloc.alloc([]u8, 0);

    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const first = leaves.items[0];
    const lower = try internal_keys.relationalJsonValueIndexPrefixAlloc(alloc, column_path, first.json_path, first.value_key);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalJsonValueIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!std.mem.eql(u8, decoded.json_path, first.json_path)) continue;
        if (!std.mem.eql(u8, decoded.value_key, first.value_key)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowJsonContains(alloc, store, decoded.doc_key, column_path, wanted)) continue;
        try out.append(alloc, try alloc.dupe(u8, decoded.doc_key));
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanJsonPathValueDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    json_path: []const u8,
    wanted: std.json.Value,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    const value_key = try jsonValueIndexKeyForValueAlloc(alloc, wanted);
    defer alloc.free(value_key);

    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalJsonValueIndexPrefixAlloc(alloc, column_path, json_path, value_key);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalJsonValueIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!std.mem.eql(u8, decoded.json_path, json_path)) continue;
        if (!std.mem.eql(u8, decoded.value_key, value_key)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowJsonPathEquals(alloc, store, decoded.doc_key, column_path, json_path, wanted)) continue;
        try out.append(alloc, try alloc.dupe(u8, decoded.doc_key));
    }

    return try out.toOwnedSlice(alloc);
}

pub fn scanJsonPathDocKeysAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    column_path: []const u8,
    json_path: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![][]u8 {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = try internal_keys.relationalJsonPathIndexPrefixAlloc(alloc, column_path, json_path);
    defer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_key| alloc.free(doc_key);
        out.deinit(alloc);
    }

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalJsonPathIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.column_path, column_path)) continue;
        if (!std.mem.eql(u8, decoded.json_path, json_path)) continue;
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        if (!try currentRowJsonPathExists(alloc, store, decoded.doc_key, column_path, json_path)) continue;
        try out.append(alloc, try alloc.dupe(u8, decoded.doc_key));
    }

    return try out.toOwnedSlice(alloc);
}

fn currentRowJsonContains(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    wanted: std.json.Value,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    return try jsonCellContains(alloc, cell, wanted);
}

fn currentRowJsonPathEquals(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    json_path: []const u8,
    wanted: std.json.Value,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    return try jsonCellPathEquals(alloc, cell, json_path, wanted);
}

fn currentRowJsonPathExists(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    column_path: []const u8,
    json_path: []const u8,
) !bool {
    const raw_row = try getRawAlloc(alloc, store, doc_key) orelse return false;
    defer alloc.free(raw_row);
    const cell = (try relational_row_codec.findCellByPath(raw_row, column_path)) orelse return false;
    return try jsonCellPathExists(alloc, cell, json_path);
}

pub fn jsonCellContains(alloc: Allocator, cell: relational_row_codec.Cell, wanted: std.json.Value) !bool {
    if (cell.value_type != .bytes_val or !cell.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    return jsonValueContains(parsed.value, wanted);
}

pub fn jsonCellPathEquals(alloc: Allocator, cell: relational_row_codec.Cell, json_path: []const u8, wanted: std.json.Value) !bool {
    if (cell.value_type != .bytes_val or !cell.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    const actual = jsonValueAtDotPath(parsed.value, json_path) orelse return false;
    return jsonValuesEqualExact(actual.*, wanted);
}

pub fn jsonCellPathExists(alloc: Allocator, cell: relational_row_codec.Cell, json_path: []const u8) !bool {
    if (cell.value_type != .bytes_val or !cell.is_json) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return false;
    defer parsed.deinit();
    return jsonValueAtDotPath(parsed.value, json_path) != null;
}

pub fn jsonValueContains(candidate: std.json.Value, wanted: std.json.Value) bool {
    return switch (wanted) {
        .object => |wanted_object| blk: {
            if (candidate != .object) break :blk false;
            for (wanted_object.keys(), wanted_object.values()) |key, wanted_value| {
                const candidate_value = candidate.object.get(key) orelse break :blk false;
                if (!jsonValueContains(candidate_value, wanted_value)) break :blk false;
            }
            break :blk true;
        },
        .array => |wanted_array| blk: {
            if (candidate != .array) break :blk false;
            for (wanted_array.items) |wanted_item| {
                var matched = false;
                for (candidate.array.items) |candidate_item| {
                    if (jsonValueContains(candidate_item, wanted_item)) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) break :blk false;
            }
            break :blk true;
        },
        else => jsonValuesEqualForContainment(candidate, wanted),
    };
}

pub fn jsonValuesEqualExact(lhs: std.json.Value, rhs: std.json.Value) bool {
    return switch (lhs) {
        .null => rhs == .null,
        .bool => |value| rhs == .bool and rhs.bool == value,
        .integer => |value| switch (rhs) {
            .integer => |other| other == value,
            .float => |other| @as(f64, @floatFromInt(value)) == other,
            else => false,
        },
        .float => |value| switch (rhs) {
            .integer => |other| value == @as(f64, @floatFromInt(other)),
            .float => |other| other == value,
            else => false,
        },
        .number_string => |value| switch (rhs) {
            .number_string => |other| std.mem.eql(u8, value, other),
            .string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .string => |value| switch (rhs) {
            .string => |other| std.mem.eql(u8, value, other),
            .number_string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .array => |array| blk: {
            if (rhs != .array or array.items.len != rhs.array.items.len) break :blk false;
            for (array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqualExact(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (rhs != .object or object.count() != rhs.object.count()) break :blk false;
            for (object.keys(), object.values()) |key, lhs_value| {
                const rhs_value = rhs.object.get(key) orelse break :blk false;
                if (!jsonValuesEqualExact(lhs_value, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

const JsonScalarOrder = enum { lt, eq, gt };

fn compareJsonScalars(lhs: std.json.Value, rhs: std.json.Value) ?JsonScalarOrder {
    return compareJsonScalarsWithCollation(lhs, rhs, null);
}

fn compareJsonScalarsWithCollation(lhs: std.json.Value, rhs: std.json.Value, collation: ?[]const u8) ?JsonScalarOrder {
    if (jsonValueAsFloat(lhs)) |left| {
        if (jsonValueAsFloat(rhs)) |right| {
            if (left < right) return .lt;
            if (left > right) return .gt;
            return .eq;
        }
    }
    if (lhs == .string and rhs == .string) {
        if (collation) |name| {
            if (relational_collation.isCaseInsensitive(name)) {
                const folded = std.ascii.orderIgnoreCase(lhs.string, rhs.string);
                return switch (folded) {
                    .lt => .lt,
                    .eq => .eq,
                    .gt => .gt,
                };
            }
        }
        return switch (std.mem.order(u8, lhs.string, rhs.string)) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    return null;
}

fn jsonValuesEqualWithCollation(lhs: std.json.Value, rhs: std.json.Value, collation: ?[]const u8) bool {
    if (lhs == .string and rhs == .string) {
        if (collation) |name| {
            if (relational_collation.isCaseInsensitive(name)) return std.ascii.eqlIgnoreCase(lhs.string, rhs.string);
        }
    }
    return jsonValuesEqualExact(lhs, rhs);
}

fn jsonValueAsFloat(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |text| std.fmt.parseFloat(f64, text) catch null,
        else => null,
    };
}

fn jsonValueAsU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @intCast(integer) else null,
        .number_string => |text| std.fmt.parseInt(u64, text, 10) catch null,
        else => null,
    };
}

fn jsonValueAsI64(value: std.json.Value) ?i64 {
    return switch (value) {
        .integer => |integer| integer,
        .number_string => |text| std.fmt.parseInt(i64, text, 10) catch null,
        else => null,
    };
}

pub fn jsonValueAtDotPath(root: std.json.Value, path: []const u8) ?*const std.json.Value {
    if (root != .object) return null;
    var current: *const std.json.Value = &root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or current.* != .object) return null;
        current = current.object.getPtr(part) orelse return null;
    }
    return current;
}

fn jsonValuesEqualForContainment(lhs: std.json.Value, rhs: std.json.Value) bool {
    return switch (lhs) {
        .null => rhs == .null,
        .bool => |value| rhs == .bool and rhs.bool == value,
        .integer => |value| switch (rhs) {
            .integer => |other| other == value,
            .float => |other| @as(f64, @floatFromInt(value)) == other,
            else => false,
        },
        .float => |value| switch (rhs) {
            .integer => |other| value == @as(f64, @floatFromInt(other)),
            .float => |other| other == value,
            else => false,
        },
        .number_string => |value| switch (rhs) {
            .number_string => |other| std.mem.eql(u8, value, other),
            .string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .string => |value| switch (rhs) {
            .string => |other| std.mem.eql(u8, value, other),
            .number_string => |other| std.mem.eql(u8, value, other),
            else => false,
        },
        .array, .object => jsonValueContains(lhs, rhs) and jsonValueContains(rhs, lhs),
    };
}

pub fn rebuildAllColumnIndexesFromRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    try rebuildAllColumnIndexesFromRowsInRangeWithColumnIndexPolicy(alloc, store, lower_doc_key, upper_doc_key, ColumnIndexPolicy.empty());
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
        try appendColumnBackedIndexWritesForRowWithColumnIndexPolicy(
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

pub fn rebuildOrderedTupleUniqueConstraintsFromRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    columns: []const schema_mod.RelationalColumn,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
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
        var decoded = try relational_row_codec.deserialize(alloc, row.row_value);
        defer decoded.deinit(alloc);
        for (unique_constraints) |constraint| {
            const index = relationalIndexForUniqueConstraint(relational_indexes, constraint, .ordered_tuple) orelse continue;
            if (!orderedTupleUniqueIndexMaintained(constraint, index)) continue;
            if (!(try rowMatchesUniqueConstraintPredicatesFromCells(alloc, decoded.cells, index.where, columns))) continue;
            if (!(try rowMatchesExpressionConditionsWithColumns(alloc, row.row_value, index.where_expressions, columns))) continue;

            var keys = (try orderedTupleUniqueIndexEntryKeysForCellsAlloc(alloc, constraint, index, row.doc_key, row.row_value, decoded.cells, columns)) orelse continue;
            defer keys.deinit(alloc);
            try requireOrderedTupleUniqueConstraintAvailableForRebuild(alloc, store, writes.items, index.name, keys.encoded_tuple, row.doc_key);
            try appendOrderedTupleIndexWriteKeys(alloc, &writes, &owned_keys, &owned_values, &keys);
        }
    }

    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
}

fn requireOrderedTupleUniqueConstraintAvailableForRebuild(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: []const docstore_mod.KVPair,
    index_id: []const u8,
    encoded_tuple: []const u8,
    doc_key: []const u8,
) !void {
    for (writes) |write| {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, write.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.index_id, index_id)) continue;
        if (!std.mem.eql(u8, decoded.encoded_tuple, encoded_tuple)) continue;
        if (std.mem.eql(u8, decoded.doc_key, doc_key)) continue;
        return error.UniqueConstraintViolation;
    }

    const committed_docs = try scanOrderedTupleDocKeysAlloc(alloc, store, index_id, encoded_tuple, "", "");
    defer freeDocKeys(alloc, committed_docs);
    for (committed_docs) |owner_doc_key| {
        if (std.mem.eql(u8, owner_doc_key, doc_key)) continue;
        return error.UniqueConstraintViolation;
    }
}

pub fn repairColumnBackedIndexesFromRowsInRangeWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !ColumnBackedIndexRepairReport {
    try validateColumnIndexPolicyForWriteMaintenance(column_index_policy);

    // Remove reverse-linked entries first, then scan forward tuple rows. The
    // forward scan catches corruption where the reverse-by-doc key is missing.
    try deleteColumnIndexesByDocRange(alloc, store, lower_doc_key, upper_doc_key);
    try pruneColumnIndexesForMissingRowsInRange(alloc, store, lower_doc_key, upper_doc_key);

    var report: ColumnBackedIndexRepairReport = .{};
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var delete_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (delete_keys.items) |key| alloc.free(key);
        delete_keys.deinit(alloc);
    }
    try appendOrderedTupleForwardIndexDeletesForDocRange(alloc, store, &deletes, &delete_keys, lower_doc_key, upper_doc_key);
    report.deleted_orphan_entries = @intCast(deletes.items.len);
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);

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
        report.scanned_rows += 1;
        const before = writes.items.len;
        try appendColumnBackedIndexWritesForRowWithColumnIndexPolicy(
            alloc,
            &writes,
            &owned_keys,
            &owned_values,
            row.doc_key,
            row.row_value,
            column_index_policy,
        );
        if (writes.items.len > before) {
            report.indexed_rows += 1;
            report.written_entries += @intCast(writes.items.len - before);
        }
    }

    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
    return report;
}

pub fn rewriteRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    plan: RowRewritePlan,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !RowRewriteReport {
    return try rewriteRowsInRangeWithColumnIndexPolicy(alloc, store, plan, lower_doc_key, upper_doc_key, ColumnIndexPolicy.empty());
}

pub fn rewriteRowsInRangeWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    plan: RowRewritePlan,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !RowRewriteReport {
    try validateRowRewritePlan(plan);

    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    var report: RowRewriteReport = .{ .scanned_rows = @intCast(rows.len) };
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

    for (rows) |row| {
        const rewritten = try rewriteRowValueAlloc(alloc, row.row_value, plan, &report);
        if (rewritten) |new_row| {
            var new_row_owned = true;
            errdefer if (new_row_owned) alloc.free(new_row);
            try appendUpsertWithColumnIndexPolicy(alloc, store, &writes, &deletes, &owned_keys, &owned_values, row.doc_key, new_row, column_index_policy);
            try owned_values.append(alloc, new_row);
            new_row_owned = false;
            report.rewritten_rows += 1;
        } else {
            report.unchanged_rows += 1;
        }
    }

    if (writes.items.len > 0 or deletes.items.len > 0) try store.putBatch(writes.items, deletes.items);
    return report;
}

pub fn rewriteRowValueWithPlanAlloc(
    alloc: Allocator,
    row_value: []const u8,
    plan: RowRewritePlan,
) !?[]u8 {
    try validateRowRewritePlan(plan);
    var report: RowRewriteReport = .{};
    return try rewriteRowValueAlloc(alloc, row_value, plan, &report);
}

fn validateRowRewritePlan(plan: RowRewritePlan) !void {
    for (plan.renames, 0..) |rename, i| {
        if (rename.old_path.len == 0 or rename.new_path.len == 0) return error.InvalidColumnValue;
        if (std.mem.eql(u8, rename.old_path, rename.new_path)) return error.InvalidColumnValue;
        for (plan.renames[i + 1 ..]) |other| {
            if (std.mem.eql(u8, rename.old_path, other.old_path)) return error.InvalidColumnValue;
            if (std.mem.eql(u8, rename.new_path, other.new_path)) return error.InvalidColumnValue;
        }
        for (plan.drops) |drop| {
            if (std.mem.eql(u8, rename.old_path, drop) or std.mem.eql(u8, rename.new_path, drop)) return error.InvalidColumnValue;
        }
    }
    for (plan.drops, 0..) |drop, i| {
        if (drop.len == 0) return error.InvalidColumnValue;
        for (plan.drops[i + 1 ..]) |other| {
            if (std.mem.eql(u8, drop, other)) return error.InvalidColumnValue;
        }
    }
    for (plan.sets, 0..) |set, i| {
        if (set.cell.path.len == 0) return error.InvalidColumnValue;
        if (!rowRewriteCellIsValid(set.cell)) return error.InvalidColumnValue;
        for (plan.drops) |drop| {
            if (std.mem.eql(u8, set.cell.path, drop)) return error.InvalidColumnValue;
        }
        for (plan.sets[i + 1 ..]) |other| {
            if (std.mem.eql(u8, set.cell.path, other.cell.path)) return error.InvalidColumnValue;
        }
    }
}

fn rewriteRowValueAlloc(
    alloc: Allocator,
    row_value: []const u8,
    plan: RowRewritePlan,
    report: *RowRewriteReport,
) !?[]u8 {
    var row = try relational_row_codec.deserialize(alloc, row_value);
    defer row.deinit(alloc);

    var cells = std.ArrayListUnmanaged(relational_row_codec.Cell).empty;
    defer cells.deinit(alloc);
    var changed = false;
    var row_renamed: u64 = 0;
    var row_dropped: u64 = 0;
    var row_set: u64 = 0;

    for (row.cells) |cell| {
        if (rowRewriteDropsPath(plan, cell.path)) {
            changed = true;
            row_dropped += 1;
            continue;
        }

        var rewritten = cell;
        if (rowRewriteRenameTarget(plan, cell.path)) |new_path| {
            rewritten.path = new_path;
            changed = true;
            row_renamed += 1;
        }
        if (rowRewriteCellsContainPath(cells.items, rewritten.path)) return error.InvalidColumnValue;
        try cells.append(alloc, rewritten);
    }

    for (plan.sets) |set| {
        if (rowRewriteCellsPathIndex(cells.items, set.cell.path)) |index| {
            if (set.only_if_missing) continue;
            cells.items[index] = set.cell;
            changed = true;
            row_set += 1;
        } else {
            try cells.append(alloc, set.cell);
            changed = true;
            row_set += 1;
        }
    }

    if (!changed) return null;
    const encoded = try relational_row_codec.serialize(alloc, cells.items);
    report.renamed_cells += row_renamed;
    report.dropped_cells += row_dropped;
    report.set_cells += row_set;
    return encoded;
}

fn rowRewriteCellIsValid(cell: relational_row_codec.Cell) bool {
    if (cell.is_json and cell.value_type != .bytes_val) return false;
    return switch (cell.value_type) {
        .u64_val => switch (cell.value) {
            .u64_val => true,
            else => false,
        },
        .i64_val => switch (cell.value) {
            .i64_val => true,
            else => false,
        },
        .f64_val => switch (cell.value) {
            .f64_val => true,
            else => false,
        },
        .bool_val => switch (cell.value) {
            .bool_val => true,
            else => false,
        },
        .geo_point => switch (cell.value) {
            .geo_point => true,
            else => false,
        },
        .bytes_val => switch (cell.value) {
            .bytes_val => true,
            else => false,
        },
    };
}

fn rowRewriteDropsPath(plan: RowRewritePlan, path: []const u8) bool {
    for (plan.drops) |drop| {
        if (std.mem.eql(u8, drop, path)) return true;
    }
    return false;
}

fn rowRewriteRenameTarget(plan: RowRewritePlan, path: []const u8) ?[]const u8 {
    for (plan.renames) |rename| {
        if (std.mem.eql(u8, rename.old_path, path)) return rename.new_path;
    }
    return null;
}

fn rowRewriteCellsContainPath(cells: []const relational_row_codec.Cell, path: []const u8) bool {
    return rowRewriteCellsPathIndex(cells, path) != null;
}

fn rowRewriteCellsPathIndex(cells: []const relational_row_codec.Cell, path: []const u8) ?usize {
    for (cells, 0..) |cell, index| {
        if (std.mem.eql(u8, cell.path, path)) return index;
    }
    return null;
}

pub fn rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    index_generation: u64,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !SecondaryIndexRebuildReport {
    var page = try rebuildColumnIndexFromRowsInSpanPageWithColumnIndexPolicy(
        alloc,
        store,
        index_name,
        index_generation,
        lower_doc_key,
        upper_doc_key,
        column_index_policy,
        std.math.maxInt(usize),
    );
    defer page.deinit(alloc);
    return page.report;
}

pub fn rebuildColumnIndexFromRowsInSpanPageWithColumnIndexPolicy(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    index_generation: u64,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
    max_rows: usize,
) !SecondaryIndexRebuildPage {
    const index = try column_index_policy.indexForRebuild(index_name, index_generation);
    const owner_column = column_index_policy.columnByName(index.owner_name) orelse return error.RelationalIndexNotFound;
    const target_policy = ColumnIndexPolicy.forIndexRebuild(column_index_policy.columns, column_index_policy.indexes, index_name, index_generation);

    var page = try scanRowsSpanPageAlloc(alloc, store, lower_doc_key, upper_doc_key, max_rows);
    defer page.deinit(alloc);

    var out = SecondaryIndexRebuildPage{
        .complete = page.next_start_doc_key.len == 0,
        .next_start_doc_key = if (page.next_start_doc_key.len > 0) try alloc.dupe(u8, page.next_start_doc_key) else "",
    };
    errdefer out.deinit(alloc);

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var delete_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (delete_keys.items) |key| alloc.free(key);
        delete_keys.deinit(alloc);
    }
    const page_upper_doc_key = if (out.next_start_doc_key.len > 0) out.next_start_doc_key else upper_doc_key;
    switch (index.access_method) {
        .scalar_column => {
            try appendColumnIndexDeletesForColumnInSpan(
                alloc,
                store,
                &deletes,
                &delete_keys,
                owner_column.path,
                lower_doc_key,
                page_upper_doc_key,
            );
        },
        .ordered_tuple => {
            if (!orderedTupleIndexRebuildable(index)) return error.InvalidSecondaryIndexRebuildRequest;
            try appendOrderedTupleIndexDeletesForIndexInSpan(
                alloc,
                store,
                &deletes,
                &delete_keys,
                index.name,
                lower_doc_key,
                page_upper_doc_key,
            );
        },
        .algebraic_filter, .text_search => return error.InvalidSecondaryIndexRebuildRequest,
    }
    out.report.deleted_entries = deletes.items.len;
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);

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

    for (page.rows) |row| {
        out.report.scanned_rows += 1;
        const before = writes.items.len;
        if (index.access_method == .scalar_column) {
            try appendColumnIndexWritesForRowWithColumnIndexPolicy(
                alloc,
                &writes,
                &owned_keys,
                &owned_values,
                row.doc_key,
                row.row_value,
                target_policy,
            );
        } else {
            var decoded = try relational_row_codec.deserialize(alloc, row.row_value);
            defer decoded.deinit(alloc);
            try appendOrderedTupleIndexWritesForRowWithColumnIndexPolicy(
                alloc,
                &writes,
                &owned_keys,
                &owned_values,
                row.doc_key,
                row.row_value,
                decoded.cells,
                target_policy,
                0,
            );
        }
        if (writes.items.len > before) {
            out.report.indexed_rows += 1;
            out.report.written_entries += @intCast(writes.items.len - before);
        }
    }

    if (writes.items.len > 0) try store.putBatch(writes.items, &.{});
    return out;
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

    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_keys = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_keys.items) |key| alloc.free(key);
        owned_keys.deinit(alloc);
    }
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_column_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_array_element_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_array_value_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_json_value_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_json_path_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_ordered_tuple_index_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendMissingRowColumnIndexDeletesForNamespace(
        alloc,
        store,
        &deletes,
        &owned_keys,
        internal_keys.relational_ordered_tuple_index_by_doc_namespace,
        lower_doc_bound,
        upper_doc_bound,
    );
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

fn appendMissingRowColumnIndexDeletesForNamespace(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    namespace: u8,
    lower_doc_bound: []const u8,
    upper_doc_bound: ?[]const u8,
) !void {
    const lower = [_]u8{namespace};
    const upper = [_]u8{namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        const doc_key = try decodedColumnIndexDocKeyAlloc(alloc, entry.key) orelse continue;
        defer alloc.free(doc_key);
        if (!(try docKeyFallsInRange(alloc, doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const row_exists = try getRawAlloc(alloc, store, doc_key);
        if (row_exists) |raw| {
            alloc.free(raw);
        } else {
            const key = try alloc.dupe(u8, entry.key);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            try owned_keys.append(alloc, key);
            key_owned = false;
            try deletes.append(alloc, key);
        }
    }
}

fn decodedColumnIndexDocKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (key.len == 0) return null;
    if (key[0] == internal_keys.relational_column_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalColumnIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_array_element_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalArrayElementIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_array_value_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalArrayValueIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_json_value_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalJsonValueIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_json_path_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalJsonPathIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_ordered_tuple_index_namespace) {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    if (key[0] == internal_keys.relational_ordered_tuple_index_by_doc_namespace) {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc, key)) orelse return null;
        defer decoded.deinit(alloc);
        return try alloc.dupe(u8, decoded.doc_key);
    }
    return null;
}

fn appendOrderedTupleForwardIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_ordered_tuple_index_namespace};
    const upper = [_]u8{internal_keys.relational_ordered_tuple_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
}

fn appendColumnIndexDeletesForColumnInSpan(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    column_path: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = if (upper_doc_key.len > 0) try internal_keys.documentRangeLowerAlloc(alloc, upper_doc_key) else null;
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        try internal_keys.relationalColumnIndexPrefixAlloc(alloc, column_path),
        null,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        try internal_keys.relationalArrayElementIndexColumnPrefixAlloc(alloc, column_path),
        null,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        try internal_keys.relationalArrayValueIndexColumnPrefixAlloc(alloc, column_path),
        null,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        try internal_keys.relationalJsonValueIndexColumnPrefixAlloc(alloc, column_path),
        null,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );
    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        try internal_keys.relationalJsonPathIndexColumnPrefixAlloc(alloc, column_path),
        null,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );

    const reverse_lower = try internal_keys.relationalColumnIndexByDocRangeLowerAlloc(alloc, lower_doc_key);
    const reverse_upper = if (upper_doc_key.len > 0)
        try internal_keys.relationalColumnIndexByDocRangeLowerAlloc(alloc, upper_doc_key)
    else
        try alloc.dupe(u8, &[_]u8{internal_keys.relational_column_index_by_doc_namespace + 1});
    try appendColumnIndexDeletesForColumnPrefix(
        alloc,
        store,
        deletes,
        owned_keys,
        reverse_lower,
        reverse_upper,
        column_path,
        lower_doc_bound,
        upper_doc_bound,
    );
}

fn appendOrderedTupleIndexDeletesForIndexInSpan(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    index_id: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const reverse_lower = try internal_keys.relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(reverse_lower);
    const reverse_upper = if (upper_doc_key.len > 0)
        try internal_keys.relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, upper_doc_key)
    else
        try alloc.dupe(u8, &[_]u8{internal_keys.relational_ordered_tuple_index_by_doc_namespace + 1});
    defer alloc.free(reverse_upper);

    const scanned = try store.scanRange(alloc, reverse_lower, reverse_upper);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.index_id, index_id)) continue;

        const reverse_key = try alloc.dupe(u8, entry.key);
        var reverse_owned = true;
        errdefer if (reverse_owned) alloc.free(reverse_key);
        try owned_keys.append(alloc, reverse_key);
        reverse_owned = false;
        try deletes.append(alloc, reverse_key);

        const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, decoded.index_id, decoded.encoded_tuple, decoded.doc_key);
        var forward_owned = true;
        errdefer if (forward_owned) alloc.free(forward_key);
        try owned_keys.append(alloc, forward_key);
        forward_owned = false;
        try deletes.append(alloc, forward_key);
    }
}

fn appendColumnIndexDeletesForColumnPrefix(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    prefix: []u8,
    explicit_upper: ?[]u8,
    column_path: []const u8,
    lower_doc_bound: []const u8,
    upper_doc_bound: ?[]const u8,
) !void {
    defer alloc.free(prefix);
    const upper = if (explicit_upper) |buf| buf else try nextPrefixAlloc(alloc, prefix);
    defer if (upper) |buf| alloc.free(buf);
    const scanned = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        if (!(try columnIndexEntryMatchesColumnAndSpan(alloc, entry.key, column_path, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
}

fn columnIndexEntryMatchesColumnAndSpan(
    alloc: Allocator,
    key: []const u8,
    column_path: []const u8,
    lower_doc_bound: []const u8,
    upper_doc_bound: ?[]const u8,
) !bool {
    if (key.len == 0) return false;
    switch (key[0]) {
        internal_keys.relational_column_index_namespace => {
            var decoded = (try internal_keys.decodeRelationalColumnIndexKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        internal_keys.relational_array_element_index_namespace => {
            var decoded = (try internal_keys.decodeRelationalArrayElementIndexKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        internal_keys.relational_array_value_index_namespace => {
            var decoded = (try internal_keys.decodeRelationalArrayValueIndexKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        internal_keys.relational_json_value_index_namespace => {
            var decoded = (try internal_keys.decodeRelationalJsonValueIndexKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        internal_keys.relational_json_path_index_namespace => {
            var decoded = (try internal_keys.decodeRelationalJsonPathIndexKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        internal_keys.relational_column_index_by_doc_namespace => {
            var decoded = (try internal_keys.decodeRelationalColumnIndexByDocKeyAlloc(alloc, key)) orelse return false;
            defer decoded.deinit(alloc);
            return std.mem.eql(u8, decoded.column_path, column_path) and try docKeyFallsInEncodedSpan(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound);
        },
        else => return false,
    }
}

fn docKeyFallsInEncodedSpan(
    alloc: Allocator,
    doc_key: []const u8,
    lower_doc_bound: []const u8,
    upper_doc_bound: ?[]const u8,
) !bool {
    const encoded = try internal_keys.documentRangeLowerAlloc(alloc, doc_key);
    defer alloc.free(encoded);
    if (std.mem.order(u8, encoded, lower_doc_bound) == .lt) return false;
    if (upper_doc_bound) |upper| {
        if (std.mem.order(u8, encoded, upper) != .lt) return false;
    }
    return true;
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
    try appendArrayElementIndexDeletesForDocRange(alloc, store, &deletes, &owned_keys, lower_doc_key, upper_doc_key);
    try appendArrayValueIndexDeletesForDocRange(alloc, store, &deletes, &owned_keys, lower_doc_key, upper_doc_key);
    try appendJsonValueIndexDeletesForDocRange(alloc, store, &deletes, &owned_keys, lower_doc_key, upper_doc_key);
    try appendJsonPathIndexDeletesForDocRange(alloc, store, &deletes, &owned_keys, lower_doc_key, upper_doc_key);
    try appendOrderedTupleIndexDeletesForDocRange(alloc, store, &deletes, &owned_keys, lower_doc_key, upper_doc_key);
    if (deletes.items.len > 0) try store.putBatch(&.{}, deletes.items);
}

fn appendOrderedTupleIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower = try internal_keys.relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower);
    const upper = try internal_keys.relationalOrderedTupleIndexByDocRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper) |buf| alloc.free(buf);

    const scanned = try store.scanRange(alloc, lower, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);

        const reverse_key = try alloc.dupe(u8, entry.key);
        var reverse_owned = true;
        errdefer if (reverse_owned) alloc.free(reverse_key);
        try owned_keys.append(alloc, reverse_key);
        reverse_owned = false;
        try deletes.append(alloc, reverse_key);

        const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, decoded.index_id, decoded.encoded_tuple, decoded.doc_key);
        var forward_owned = true;
        errdefer if (forward_owned) alloc.free(forward_key);
        try owned_keys.append(alloc, forward_key);
        forward_owned = false;
        try deletes.append(alloc, forward_key);
    }
}

fn appendArrayElementIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_array_element_index_namespace};
    const upper = [_]u8{internal_keys.relational_array_element_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalArrayElementIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
}

fn appendArrayValueIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_array_value_index_namespace};
    const upper = [_]u8{internal_keys.relational_array_value_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalArrayValueIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
}

fn appendJsonValueIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_json_value_index_namespace};
    const upper = [_]u8{internal_keys.relational_json_value_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalJsonValueIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
}

fn appendJsonPathIndexDeletesForDocRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    const lower_doc_bound = try internal_keys.documentRangeLowerAlloc(alloc, lower_doc_key);
    defer alloc.free(lower_doc_bound);
    const upper_doc_bound = try internal_keys.documentRangeUpperAlloc(alloc, upper_doc_key);
    defer if (upper_doc_bound) |buf| alloc.free(buf);

    const lower = [_]u8{internal_keys.relational_json_path_index_namespace};
    const upper = [_]u8{internal_keys.relational_json_path_index_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalJsonPathIndexKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!(try docKeyFallsInRange(alloc, decoded.doc_key, lower_doc_bound, upper_doc_bound))) continue;
        const key = try alloc.dupe(u8, entry.key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        try owned_keys.append(alloc, key);
        key_owned = false;
        try deletes.append(alloc, key);
    }
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
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
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
            if (!uniqueConstraintUsesDedicatedOwnerRowsWithIndexes(constraint, relational_indexes)) continue;
            const encoded_value = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row.row_value, constraint, columns)) orelse continue;
            defer alloc.free(encoded_value);

            const key = if (constraint.without_overlaps_period) |period_name| blk: {
                const span = try periodSpanForRowWithCatalog(alloc, columns, periods, row.row_value, period_name);
                try requireTemporalUniqueAvailableInBatchAndStore(alloc, store, writes.items, &.{}, &.{}, constraint, encoded_value, span, row.doc_key);
                break :blk try temporalUniqueConstraintKeyAlloc(alloc, constraint, encoded_value, span, row.doc_key);
            } else try internal_keys.relationalUniqueKeyAlloc(alloc, constraint.name, encoded_value);
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

pub fn reconcileUniqueConstraintRowsInRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
) !UniqueConstraintIntegrityReport {
    var report: UniqueConstraintIntegrityReport = .{};
    if (unique_constraints.len == 0) return report;

    const rows = try scanRowsAlloc(alloc, store, lower_doc_key, upper_doc_key);
    defer freeRows(alloc, rows);

    var expected = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer expected.deinit(alloc);
    var duplicate_expected_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer duplicate_expected_keys.deinit(alloc);
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
        report.scanned_rows += 1;
        for (unique_constraints) |constraint| {
            if (!uniqueConstraintUsesDedicatedOwnerRowsWithIndexes(constraint, relational_indexes)) continue;
            const encoded_value = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row.row_value, constraint, columns)) orelse continue;
            defer alloc.free(encoded_value);
            const key = if (constraint.without_overlaps_period) |period_name| blk: {
                const span = try periodSpanForRowWithCatalog(alloc, columns, periods, row.row_value, period_name);
                break :blk try temporalUniqueConstraintKeyAlloc(alloc, constraint, encoded_value, span, row.doc_key);
            } else try internal_keys.relationalUniqueKeyAlloc(alloc, constraint.name, encoded_value);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            if (batchWriteValue(expected.items, key)) |existing_owner| {
                if (!std.mem.eql(u8, existing_owner, row.doc_key)) {
                    report.duplicate_unique_rows += 1;
                    if (!containsKey(duplicate_expected_keys.items, key)) try duplicate_expected_keys.append(alloc, key);
                }
                alloc.free(key);
                continue;
            }
            const owner = try alloc.dupe(u8, row.doc_key);
            var owner_owned = true;
            errdefer if (owner_owned) alloc.free(owner);
            try owned_keys.append(alloc, key);
            key_owned = false;
            try owned_values.append(alloc, owner);
            owner_owned = false;
            try expected.append(alloc, .{ .key = key, .value = owner });
            report.expected_unique_rows += 1;
        }
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer writes.deinit(alloc);
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);

    for (expected.items) |entry| {
        const committed_owner = store.get(alloc, entry.key) catch |err| switch (err) {
            error.NotFound => {
                report.missing_unique_rows += 1;
                if (mode == .repair or mode == .dry_run) {
                    report.repaired_unique_rows += 1;
                    if (mode == .repair) try writes.append(alloc, entry);
                }
                continue;
            },
            else => return err,
        };
        defer alloc.free(committed_owner);
        if (std.mem.eql(u8, committed_owner, entry.value)) continue;
        report.stale_unique_rows += 1;
        if (!containsKey(duplicate_expected_keys.items, entry.key) and (mode == .repair or mode == .dry_run)) {
            report.repaired_unique_rows += 1;
            if (mode == .repair) try writes.append(alloc, entry);
        }
    }

    const lower = [_]u8{internal_keys.relational_unique_namespace};
    const upper = [_]u8{internal_keys.relational_unique_namespace + 1};
    const scanned_unique = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned_unique);
    for (scanned_unique) |entry| {
        const constraint_name = (try decodeRelationalUniqueConstraintNameAlloc(alloc, entry.key)) orelse continue;
        defer alloc.free(constraint_name);
        if (findUniqueConstraintNameInCatalog(unique_constraints, constraint_name)) |constraint| {
            if (!uniqueConstraintUsesDedicatedOwnerRowsWithIndexes(constraint, relational_indexes)) {
                report.stale_unique_rows += 1;
                if (mode == .repair or mode == .dry_run) {
                    report.deleted_stale_unique_rows += 1;
                    if (mode == .repair) try deletes.append(alloc, entry.key);
                }
                continue;
            }
        } else continue;
        report.scanned_unique_rows += 1;
        if (batchWriteValue(expected.items, entry.key)) |expected_owner| {
            if (std.mem.eql(u8, expected_owner, entry.value)) continue;
            if (containsKey(duplicate_expected_keys.items, entry.key)) continue;
            continue;
        }
        report.stale_unique_rows += 1;
        if (mode == .repair or mode == .dry_run) {
            report.deleted_stale_unique_rows += 1;
            if (mode == .repair) try deletes.append(alloc, entry.key);
        }
    }

    const temporal_lower = [_]u8{internal_keys.relational_temporal_unique_namespace};
    const temporal_upper = [_]u8{internal_keys.relational_temporal_unique_namespace + 1};
    const scanned_temporal = try store.scanRange(alloc, temporal_lower[0..], temporal_upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned_temporal);
    for (scanned_temporal) |entry| {
        const constraint_name = (try decodeRelationalTemporalUniqueConstraintNameAlloc(alloc, entry.key)) orelse continue;
        defer alloc.free(constraint_name);
        if (findUniqueConstraintNameInCatalog(unique_constraints, constraint_name)) |constraint| {
            if (!uniqueConstraintUsesDedicatedOwnerRowsWithIndexes(constraint, relational_indexes)) continue;
        } else continue;
        report.scanned_unique_rows += 1;
        if (batchWriteValue(expected.items, entry.key)) |expected_owner| {
            if (std.mem.eql(u8, expected_owner, entry.value)) continue;
            if (containsKey(duplicate_expected_keys.items, entry.key)) continue;
            continue;
        }
        report.stale_unique_rows += 1;
        if (mode == .repair or mode == .dry_run) {
            report.deleted_stale_unique_rows += 1;
            if (mode == .repair) try deletes.append(alloc, entry.key);
        }
    }

    if (mode == .repair and (writes.items.len > 0 or deletes.items.len > 0)) {
        try store.putBatch(writes.items, deletes.items);
    }
    return report;
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
        _ = findUniqueConstraintNameInCatalog(unique_constraints, constraint_name) orelse continue;
        try deletes.append(alloc, entry.key);
    }
    const temporal_lower = [_]u8{internal_keys.relational_temporal_unique_namespace};
    const temporal_upper = [_]u8{internal_keys.relational_temporal_unique_namespace + 1};
    const scanned_temporal = try store.scanRange(alloc, temporal_lower[0..], temporal_upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned_temporal);
    for (scanned_temporal) |entry| {
        const constraint_name = (try decodeRelationalTemporalUniqueConstraintNameAlloc(alloc, entry.key)) orelse continue;
        defer alloc.free(constraint_name);
        if (findUniqueConstraintNameInCatalog(unique_constraints, constraint_name)) |constraint| {
            if (!uniqueConstraintUsesDedicatedOwnerRows(constraint)) continue;
        } else continue;
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

pub fn reconcileForeignKeyRefOwnerRange(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    constraint_name: []const u8,
    parent_table: []const u8,
    start_parent_key: []const u8,
    end_parent_key: []const u8,
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

    const lower = [_]u8{internal_keys.relational_foreign_key_ref_namespace};
    const upper = [_]u8{internal_keys.relational_foreign_key_ref_namespace + 1};
    const scanned = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, scanned);

    for (scanned) |entry| {
        var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.constraint_name, constraint_name)) continue;
        if (!std.mem.eql(u8, decoded.parent_table, parent_table)) continue;
        if (!std.mem.eql(u8, decoded.child_table, table_name)) continue;
        if (!parentKeyFallsInOwnerRange(decoded.parent_key, start_parent_key, end_parent_key)) continue;
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

fn parentKeyFallsInOwnerRange(parent_key: []const u8, start_parent_key: []const u8, end_parent_key: []const u8) bool {
    if (start_parent_key.len > 0 and std.mem.order(u8, parent_key, start_parent_key) == .lt) return false;
    if (end_parent_key.len > 0 and std.mem.order(u8, parent_key, end_parent_key) != .lt) return false;
    return true;
}

pub fn explainForeignKeyDelete(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    foreign_keys: []const schema_mod.ForeignKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    doc_key: []const u8,
) !ForeignKeyDeletePlan {
    return try explainForeignKeyDeleteWithPrimaryKey(alloc, store, table_name, &.{}, &.{}, foreign_keys, null, unique_constraints, doc_key);
}

pub fn explainForeignKeyDeleteWithPrimaryKey(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
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
    participant.configurePeriods(periods, columns);
    participant.configureForeignKeys(table_name, foreign_keys, &.{doc_key});
    participant.configurePrimaryKey(primary_key);
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
    return try listForeignKeyViolationsInRangeWithPrimaryKey(alloc, store, table_name, &.{}, &.{}, foreign_keys, null, unique_constraints, lower_doc_key, upper_doc_key);
}

pub fn listForeignKeyViolationsInRangeWithPrimaryKey(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
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
        columns,
        periods,
        foreign_keys,
        primary_key,
        unique_constraints,
        &.{},
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
    return try reconcileForeignKeyRefsInRangeWithPrimaryKey(alloc, store, table_name, &.{}, &.{}, foreign_keys, null, unique_constraints, lower_doc_key, upper_doc_key, mode);
}

pub fn reconcileForeignKeyRefsInRangeWithPrimaryKey(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRangeWithPrimaryKeyAndIndexes(alloc, store, table_name, columns, periods, foreign_keys, primary_key, unique_constraints, &.{}, lower_doc_key, upper_doc_key, mode);
}

pub fn reconcileForeignKeyRefsInRangeWithPrimaryKeyAndIndexes(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    mode: ForeignKeyIntegrityMode,
) !ForeignKeyIntegrityReport {
    return try reconcileForeignKeyRefsInRangeWithViolations(alloc, store, table_name, columns, periods, foreign_keys, primary_key, unique_constraints, relational_indexes, lower_doc_key, upper_doc_key, mode, null);
}

fn reconcileForeignKeyRefsInRangeWithViolations(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    table_name: []const u8,
    columns: []const schema_mod.RelationalColumn,
    periods: []const schema_mod.RelationalPeriod,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
    unique_constraints: []const schema_mod.UniqueConstraint,
    relational_indexes: []const schema_mod.RelationalIndex,
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
            const parent_key = (try foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(alloc, row.row_value, foreign_key, primary_key, columns)) orelse continue;
            defer alloc.free(parent_key);
            report.referenced_child_rows += 1;

            if (!(try foreignKeyParentExistsForChildRow(alloc, store, relational_indexes, columns, periods, row.row_value, foreign_key, primary_key, unique_constraints, parent_key))) {
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
        columns,
        foreign_keys,
        primary_key,
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
    columns: []const schema_mod.RelationalColumn,
    foreign_keys: []const schema_mod.ForeignKey,
    primary_key: ?schema_mod.PrimaryKey,
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
            if (try foreignKeyReferenceValueWithColumnsAndPrimaryKeyAlloc(alloc, raw, foreign_key, primary_key, columns)) |current_parent| {
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

fn decodeRelationalTemporalUniqueConstraintNameAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!internal_keys.isRelationalTemporalUniqueKey(key)) return null;
    const constraint_term = internal_keys.findComponentTerminator(key, 1) orelse return error.InvalidInternalUserKey;
    return try internal_keys.decodeBodyAlloc(alloc, key[1..constraint_term]);
}

fn uniqueConstraintNameInCatalog(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) bool {
    return findUniqueConstraintNameInCatalog(unique_constraints, name) != null;
}

fn findUniqueConstraintNameInCatalog(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) ?schema_mod.UniqueConstraint {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn foreignKeyNameInCatalog(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) bool {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return true;
    }
    return false;
}

fn clearColumnIndexNamespace(alloc: Allocator, store: *docstore_mod.DocStore) !void {
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_column_index_namespace);
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_array_element_index_namespace);
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_array_value_index_namespace);
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_json_value_index_namespace);
    try clearColumnIndexNamespacePrefix(alloc, store, internal_keys.relational_json_path_index_namespace);
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
        if (!internal_keys.isRelationalColumnIndexKey(entry.key) and
            !internal_keys.isRelationalArrayElementIndexKey(entry.key) and
            !internal_keys.isRelationalArrayValueIndexKey(entry.key) and
            !internal_keys.isRelationalJsonValueIndexKey(entry.key) and
            !internal_keys.isRelationalJsonPathIndexKey(entry.key) and
            !internal_keys.isRelationalOrderedTupleIndexKey(entry.key) and
            !internal_keys.isRelationalOrderedTupleIndexByDocKey(entry.key) and
            !internal_keys.isRelationalColumnIndexByDocKey(entry.key)) continue;
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
    const old_row_value = try getRawAlloc(alloc, store, doc_key);
    defer if (old_row_value) |value| alloc.free(value);
    try appendUpsertInternalWithOldRow(alloc, writes, deletes, owned_keys, owned_values, doc_key, old_row_value, row_value, column_index_policy, 0);
}

fn appendUpsertInternalWithOldRow(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    row_value: []const u8,
    column_index_policy: ColumnIndexPolicy,
    row_generation: u64,
) !void {
    if (!column_index_policy.mayIndexAnyRow() or
        (old_row_value != null and std.mem.eql(u8, old_row_value.?, row_value) and column_index_policy.canSkipUnchangedRowDerivedMaintenance()))
    {
        return try appendUpsertInternalWithOldRowAndCells(alloc, writes, deletes, owned_keys, owned_values, doc_key, old_row_value, null, row_value, null, column_index_policy, row_generation);
    }

    var new_row = try relational_row_codec.deserialize(alloc, row_value);
    defer new_row.deinit(alloc);
    var old_row: relational_row_codec.Row = undefined;
    var old_row_ready = false;
    defer if (old_row_ready) old_row.deinit(alloc);
    if (old_row_value) |old_value| {
        old_row = try relational_row_codec.deserialize(alloc, old_value);
        old_row_ready = true;
    }
    return try appendUpsertInternalWithOldRowAndCells(
        alloc,
        writes,
        deletes,
        owned_keys,
        owned_values,
        doc_key,
        old_row_value,
        if (old_row_ready) old_row.cells else null,
        row_value,
        new_row.cells,
        column_index_policy,
        row_generation,
    );
}

fn appendUpsertInternalWithOldRowAndCells(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    old_cells: ?[]const relational_row_codec.Cell,
    row_value: []const u8,
    new_cells: ?[]const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
    row_generation: u64,
) !void {
    const may_index_any_row = column_index_policy.mayIndexAnyRow();
    if (may_index_any_row) try validateColumnIndexPolicyForWriteMaintenance(column_index_policy);

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

    if (old_row_value) |old_value| {
        if (std.mem.eql(u8, old_value, row_value) and column_index_policy.canSkipUnchangedRowDerivedMaintenance()) return;
    }

    if (!may_index_any_row) return;

    const resolved_new_cells = new_cells orelse return error.InvalidArgument;

    if (old_row_value) |old_value| {
        const resolved_old_cells = old_cells orelse return error.InvalidArgument;
        try appendChangedColumnIndexMutationsForRows(
            alloc,
            writes,
            deletes,
            owned_keys,
            owned_values,
            doc_key,
            old_value,
            resolved_old_cells,
            row_value,
            resolved_new_cells,
            column_index_policy,
        );
        try appendChangedOrderedTupleIndexMutationsForRows(
            alloc,
            writes,
            deletes,
            owned_keys,
            owned_values,
            doc_key,
            old_value,
            resolved_old_cells,
            row_value,
            resolved_new_cells,
            column_index_policy,
            row_generation,
        );
    } else {
        const reserve = insertIndexReserveCounts(column_index_policy, resolved_new_cells);
        try writes.ensureUnusedCapacity(alloc, reserve.writes);
        if (owned_keys) |keys| try keys.ensureUnusedCapacity(alloc, reserve.keys);
        if (owned_values) |values| try values.ensureUnusedCapacity(alloc, reserve.values);
        for (resolved_new_cells) |cell| {
            if (try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, row_value)) {
                try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, cell);
            }
        }
        try appendOrderedTupleIndexWritesForRowWithColumnIndexPolicy(alloc, writes, owned_keys, owned_values, doc_key, row_value, resolved_new_cells, column_index_policy, row_generation);
    }
}

const InsertIndexReserveCounts = struct {
    writes: usize = 0,
    keys: usize = 0,
    values: usize = 0,
};

fn insertIndexReserveCounts(
    column_index_policy: ColumnIndexPolicy,
    cells: []const relational_row_codec.Cell,
) InsertIndexReserveCounts {
    var scalar_cells: usize = cells.len;
    if (!column_index_policy.maintainsLegacyScalarIndexes()) {
        scalar_cells = 0;
        for (cells) |cell| {
            for (column_index_policy.columns) |column| {
                if (!std.mem.eql(u8, column.path, cell.path) and !std.mem.eql(u8, column.name, cell.path)) continue;
                if (column_index_policy.columnHasScalarIndex(column)) scalar_cells += 1;
                break;
            }
        }
    }

    var ordered_indexes: usize = 0;
    for (column_index_policy.indexes) |index| {
        if (index.owner_kind != .relational_column) continue;
        if (!column_index_policy.shouldMaintainCatalogIndex(index)) continue;
        if (!orderedTupleIndexMaintained(index)) continue;
        ordered_indexes += 1;
    }

    return .{
        .writes = scalar_cells * 6 + ordered_indexes * 2,
        .keys = scalar_cells * 5 + ordered_indexes * 2,
        .values = scalar_cells + ordered_indexes,
    };
}

const ChangedScalarIndexReserveCounts = struct {
    writes: usize = 0,
    deletes: usize = 0,
    keys: usize = 0,
    values: usize = 0,
};

fn changedScalarIndexReserveCounts(
    column_index_policy: ColumnIndexPolicy,
    old_cells: []const relational_row_codec.Cell,
    new_cells: []const relational_row_codec.Cell,
) ChangedScalarIndexReserveCounts {
    const old_scalar_cells = scalarIndexCellCount(column_index_policy, old_cells);
    const new_scalar_cells = scalarIndexCellCount(column_index_policy, new_cells);
    return .{
        .writes = new_scalar_cells * 6,
        .deletes = old_scalar_cells * 5,
        .keys = old_scalar_cells * 5 + new_scalar_cells * 5,
        .values = new_scalar_cells,
    };
}

fn scalarIndexCellCount(
    column_index_policy: ColumnIndexPolicy,
    cells: []const relational_row_codec.Cell,
) usize {
    var count: usize = 0;
    for (cells) |cell| {
        if (catalogCellHasScalarIndex(column_index_policy, cell.path)) count += 1;
    }
    return count;
}

fn catalogCellHasScalarIndex(column_index_policy: ColumnIndexPolicy, path: []const u8) bool {
    if (column_index_policy.maintainsLegacyScalarIndexes()) return true;
    for (column_index_policy.columns) |column| {
        if (!std.mem.eql(u8, column.path, path) and !std.mem.eql(u8, column.name, path)) continue;
        return column_index_policy.columnHasScalarIndex(column);
    }
    return false;
}

fn appendChangedOrderedTupleIndexMutationsForRows(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: []const u8,
    old_cells: []const relational_row_codec.Cell,
    new_row_value: []const u8,
    new_cells: []const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
    row_generation: u64,
) !void {
    for (column_index_policy.indexes) |index| {
        if (index.owner_kind != .relational_column) continue;
        if (!column_index_policy.shouldMaintainCatalogIndex(index)) continue;
        if (column_index_policy.orderedTupleIndexOwnerColumn(index) == null) continue;
        if (!(try orderedTupleIndexMaintainedChecked(index))) continue;
        const old_indexed = try column_index_policy.shouldIndexCatalogRow(alloc, index, old_row_value);
        const new_indexed = try column_index_policy.shouldIndexCatalogRow(alloc, index, new_row_value);
        if (old_indexed) {
            var old_keys = try orderedTupleIndexEntryKeysForCellsAlloc(alloc, index.name, doc_key, old_cells, index.keys, index.include_columns, column_index_policy.columns);
            defer old_keys.deinit(alloc);
            if (!new_indexed) {
                try appendOrderedTupleIndexDeleteKeys(alloc, deletes, owned_keys, &old_keys);
                continue;
            }
            var new_keys = try orderedTupleIndexEntryKeysForCellsWithMetadataAlloc(alloc, index.name, doc_key, new_cells, index.keys, index.include_columns, column_index_policy.columns, row_generation, index.generation, index.schema_fingerprint);
            defer new_keys.deinit(alloc);
            if (std.mem.eql(u8, old_keys.encoded_tuple, new_keys.encoded_tuple)) {
                if (!std.mem.eql(u8, old_keys.forward_value, new_keys.forward_value)) {
                    try appendOrderedTupleForwardIndexWriteKey(alloc, writes, owned_keys, owned_values, &new_keys);
                }
                continue;
            }
            try appendOrderedTupleIndexDeleteKeys(alloc, deletes, owned_keys, &old_keys);
            try appendOrderedTupleIndexWriteKeys(alloc, writes, owned_keys, owned_values, &new_keys);
        } else if (new_indexed) {
            var new_keys = try orderedTupleIndexEntryKeysForCellsWithMetadataAlloc(alloc, index.name, doc_key, new_cells, index.keys, index.include_columns, column_index_policy.columns, row_generation, index.generation, index.schema_fingerprint);
            defer new_keys.deinit(alloc);
            try appendOrderedTupleIndexWriteKeys(alloc, writes, owned_keys, owned_values, &new_keys);
        }
    }
}

fn appendChangedColumnIndexMutationsForRows(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: []const u8,
    old_cells: []const relational_row_codec.Cell,
    new_row_value: []const u8,
    new_cells: []const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
) !void {
    const reserve = changedScalarIndexReserveCounts(column_index_policy, old_cells, new_cells);
    try writes.ensureUnusedCapacity(alloc, reserve.writes);
    try deletes.ensureUnusedCapacity(alloc, reserve.deletes);
    if (owned_keys) |keys| try keys.ensureUnusedCapacity(alloc, reserve.keys);
    if (owned_values) |values| try values.ensureUnusedCapacity(alloc, reserve.values);

    for (old_cells) |old_cell| {
        const old_indexed = try column_index_policy.shouldMaintainScalarIndexRow(alloc, old_cell.path, old_row_value);
        const new_cell = findCellInCells(new_cells, old_cell.path);
        const new_indexed = if (new_cell) |cell|
            try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, new_row_value)
        else
            false;
        if (!old_indexed) continue;
        if (!new_indexed or !relationalCellsEqual(old_cell, new_cell.?)) {
            try appendColumnIndexDeleteForCell(alloc, deletes, owned_keys, doc_key, old_cell);
        }
    }

    for (new_cells) |new_cell| {
        const new_indexed = try column_index_policy.shouldMaintainScalarIndexRow(alloc, new_cell.path, new_row_value);
        if (!new_indexed) continue;
        const old_cell = findCellInCells(old_cells, new_cell.path);
        const old_indexed = if (old_cell) |cell|
            try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, old_row_value)
        else
            false;
        if (!old_indexed or !relationalCellsEqual(old_cell.?, new_cell)) {
            try appendColumnIndexWriteForCell(alloc, writes, owned_keys, owned_values, doc_key, new_cell);
        }
    }
}

fn appendOrderedTupleIndexWritesForRowWithColumnIndexPolicy(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
    row_generation: u64,
) !void {
    for (column_index_policy.indexes) |index| {
        if (index.owner_kind != .relational_column) continue;
        if (!column_index_policy.shouldMaintainCatalogIndex(index)) continue;
        if (column_index_policy.orderedTupleIndexOwnerColumn(index) == null) continue;
        if (!(try orderedTupleIndexMaintainedChecked(index))) continue;
        if (!(try column_index_policy.shouldIndexCatalogRow(alloc, index, row_value))) continue;
        var keys = try orderedTupleIndexEntryKeysForCellsWithMetadataAlloc(alloc, index.name, doc_key, cells, index.keys, index.include_columns, column_index_policy.columns, row_generation, index.generation, index.schema_fingerprint);
        defer keys.deinit(alloc);
        try appendOrderedTupleIndexWriteKeys(alloc, writes, owned_keys, owned_values, &keys);
    }
}

fn appendOrderedTupleIndexDeletesForRowWithColumnIndexPolicy(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    row_value: []const u8,
    cells: []const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
) !void {
    for (column_index_policy.indexes) |index| {
        if (index.owner_kind != .relational_column) continue;
        if (!column_index_policy.shouldMaintainCatalogIndex(index)) continue;
        if (column_index_policy.orderedTupleIndexOwnerColumn(index) == null) continue;
        if (!(try orderedTupleIndexMaintainedChecked(index))) continue;
        if (!(try column_index_policy.shouldIndexCatalogRow(alloc, index, row_value))) continue;
        var keys = try orderedTupleIndexEntryKeysForCellsAlloc(alloc, index.name, doc_key, cells, index.keys, index.include_columns, column_index_policy.columns);
        defer keys.deinit(alloc);
        try appendOrderedTupleIndexDeleteKeys(alloc, deletes, owned_keys, &keys);
    }
}

fn orderedTupleIndexMaintained(index: schema_mod.RelationalIndex) bool {
    return index.access_method == .ordered_tuple and
        index.keys.len != 0 and
        relationalIndexMaintained(index);
}

fn orderedTupleIndexMaintainedChecked(index: schema_mod.RelationalIndex) !bool {
    if (!orderedTupleIndexMaintained(index)) return false;
    if (index.generation == 0) return error.RelationalIndexGenerationMismatch;
    if (index.schema_fingerprint == null or index.schema_fingerprint.?.len == 0) return error.InvalidSchemaUpdateRequest;
    return true;
}

fn validateColumnIndexPolicyForWriteMaintenance(column_index_policy: ColumnIndexPolicy) !void {
    for (column_index_policy.indexes) |index| {
        if (index.owner_kind != .relational_column) continue;
        if (column_index_policy.orderedTupleIndexOwnerColumn(index) == null) continue;
        if (!column_index_policy.shouldMaintainCatalogIndex(index)) continue;
        _ = try orderedTupleIndexMaintainedChecked(index);
    }
}

fn orderedTupleIndexRebuildable(index: schema_mod.RelationalIndex) bool {
    return index.access_method == .ordered_tuple and
        index.keys.len != 0 and
        relationalIndexRebuildable(index);
}

fn orderedTupleUniqueIndexMaintained(constraint: schema_mod.UniqueConstraint, index: schema_mod.RelationalIndex) bool {
    return index.access_method == .ordered_tuple and
        (index.keys.len != 0 or constraint.expressions.len != 0) and
        constraint.without_overlaps_period == null and
        relationalIndexMaintained(index);
}

pub fn orderedTupleUniqueIndexLookupReady(constraint: schema_mod.UniqueConstraint, index: schema_mod.RelationalIndex) bool {
    const expression_keys_match = blk: {
        if (index.expressions.len != constraint.expressions.len) break :blk false;
        for (index.expressions, constraint.expressions) |index_expression, constraint_expression| {
            if (index_expression.op != constraint_expression.op) break :blk false;
            if (!std.mem.eql(u8, index_expression.field, constraint_expression.field)) break :blk false;
            if (index_expression.expression != null or constraint_expression.expression != null) break :blk false;
        }
        break :blk true;
    };
    return constraint.validation_state == .enforced and
        index.access_method == .ordered_tuple and
        index.keys.len == constraint.columns.len and
        (index.keys.len != 0 or constraint.expressions.len != 0) and
        expression_keys_match and
        index.generation != 0 and
        index.schema_fingerprint != null and
        index.schema_fingerprint.?.len != 0 and
        constraint.without_overlaps_period == null and
        relationalIndexReady(index);
}

fn orderedTupleIndexReadyForConstraintLookup(index: schema_mod.RelationalIndex) bool {
    return relationalIndexReady(index) and
        index.generation != 0 and
        index.schema_fingerprint != null and
        index.schema_fingerprint.?.len != 0;
}

fn orderedTupleIndexCoversForeignKeyChildColumns(index: schema_mod.RelationalIndex, foreign_key: schema_mod.ForeignKey) bool {
    if (index.keys.len != foreign_key.child_columns.len or index.keys.len == 0) return false;
    for (foreign_key.child_columns) |child_column| {
        var found = false;
        for (index.keys) |index_key| {
            if (std.mem.eql(u8, index_key.column, child_column)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn foreignKeyParentColumnIndexForOrderedKey(
    columns: []const schema_mod.RelationalColumn,
    foreign_key: schema_mod.ForeignKey,
    ordered_key_column: []const u8,
) ?usize {
    const ordered_column = findRelationalColumn(columns, ordered_key_column) orelse return null;
    for (foreign_key.parent_columns, 0..) |parent_column_name, index| {
        const parent_column = findRelationalColumn(columns, parent_column_name) orelse return null;
        if (std.mem.eql(u8, parent_column.name, ordered_column.name) or std.mem.eql(u8, parent_column.path, ordered_column.path)) return index;
    }
    return null;
}

fn relationalIndexLifecycleMaintained(lifecycle: schema_mod.RelationalIndexLifecycle) bool {
    return switch (lifecycle) {
        .ready, .building, .catching_up => true,
        .invalid, .dropping, .stale, .rebuild_required, .failed => false,
    };
}

fn relationalIndexLifecycleRebuildable(lifecycle: schema_mod.RelationalIndexLifecycle) bool {
    return switch (lifecycle) {
        .building, .rebuild_required => true,
        .ready, .catching_up, .invalid, .dropping, .stale, .failed => false,
    };
}

fn relationalIndexMaintained(index: schema_mod.RelationalIndex) bool {
    const lifecycle = schema_mod.relationalIndexLifecycle(index) orelse return false;
    return relationalIndexLifecycleMaintained(lifecycle);
}

fn relationalIndexRebuildable(index: schema_mod.RelationalIndex) bool {
    const lifecycle = schema_mod.relationalIndexLifecycle(index) orelse return false;
    return relationalIndexLifecycleRebuildable(lifecycle);
}

fn relationalIndexReady(index: schema_mod.RelationalIndex) bool {
    const lifecycle = schema_mod.relationalIndexLifecycle(index) orelse return false;
    return lifecycle == .ready;
}

fn orderedTupleIndexId(column: schema_mod.RelationalColumn) []const u8 {
    return column.index_name orelse column.name;
}

fn appendOrderedTupleIndexWriteKeys(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    keys: *OrderedTupleIndexEntryKeys,
) !void {
    try appendOrderedTupleForwardIndexWriteKey(alloc, writes, owned_keys, owned_values, keys);

    const reverse_key = try stageOrderedTupleBuffer(alloc, &keys.reverse_key, owned_keys);
    var reverse_owned = owned_keys == null;
    errdefer if (reverse_owned) alloc.free(reverse_key);
    const reverse_value: []const u8 = if (owned_values == null) try alloc.dupe(u8, "") else "";
    var reverse_value_owned = owned_values == null;
    errdefer if (reverse_value_owned) alloc.free(@constCast(reverse_value));
    try writes.append(alloc, .{ .key = reverse_key, .value = reverse_value });
    reverse_owned = false;
    reverse_value_owned = false;
}

fn appendOrderedTupleForwardIndexWriteKey(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    owned_values: ?*std.ArrayListUnmanaged([]u8),
    keys: *OrderedTupleIndexEntryKeys,
) !void {
    const forward_key = try stageOrderedTupleBuffer(alloc, &keys.forward_key, owned_keys);
    var forward_owned = owned_keys == null;
    errdefer if (forward_owned) alloc.free(forward_key);
    const forward_value = try stageOrderedTupleBuffer(alloc, &keys.forward_value, owned_values);
    var value_owned = owned_values == null;
    errdefer if (value_owned) alloc.free(forward_value);
    try writes.append(alloc, .{ .key = forward_key, .value = forward_value });
    forward_owned = false;
    value_owned = false;
}

fn appendOrderedTupleIndexDeleteKeys(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    keys: *OrderedTupleIndexEntryKeys,
) !void {
    const forward_key = try stageOrderedTupleBuffer(alloc, &keys.forward_key, owned_keys);
    var forward_owned = owned_keys == null;
    errdefer if (forward_owned) alloc.free(forward_key);
    try deletes.append(alloc, forward_key);
    forward_owned = false;

    const reverse_key = try stageOrderedTupleBuffer(alloc, &keys.reverse_key, owned_keys);
    var reverse_owned = owned_keys == null;
    errdefer if (reverse_owned) alloc.free(reverse_key);
    try deletes.append(alloc, reverse_key);
    reverse_owned = false;
}

fn stageOrderedTupleBuffer(
    alloc: Allocator,
    source: *[]u8,
    owned: ?*std.ArrayListUnmanaged([]u8),
) ![]u8 {
    if (owned) |items| {
        const staged = source.*;
        try items.append(alloc, staged);
        source.* = source.*[0..0];
        return staged;
    }
    return try alloc.dupe(u8, source.*);
}

fn findCellInCells(cells: []const relational_row_codec.Cell, path: []const u8) ?relational_row_codec.Cell {
    for (cells) |cell| {
        if (std.mem.eql(u8, cell.path, path)) return cell;
    }
    return null;
}

fn relationalCellsEqual(lhs: relational_row_codec.Cell, rhs: relational_row_codec.Cell) bool {
    if (!std.mem.eql(u8, lhs.path, rhs.path)) return false;
    if (lhs.value_type != rhs.value_type or lhs.is_json != rhs.is_json) return false;
    return switch (lhs.value) {
        .u64_val => |value| rhs.value == .u64_val and rhs.value.u64_val == value,
        .i64_val => |value| rhs.value == .i64_val and rhs.value.i64_val == value,
        .f64_val => |value| rhs.value == .f64_val and rhs.value.f64_val == value,
        .bool_val => |value| rhs.value == .bool_val and rhs.value.bool_val == value,
        .geo_point => |value| rhs.value == .geo_point and rhs.value.geo_point.lat == value.lat and rhs.value.geo_point.lon == value.lon,
        .bytes_val => |value| rhs.value == .bytes_val and std.mem.eql(u8, rhs.value.bytes_val, value),
    };
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

    try appendArrayElementIndexWritesForCell(alloc, writes, owned_keys, doc_key, cell);
    try appendArrayValueIndexWriteForCell(alloc, writes, owned_keys, doc_key, cell);
    try appendJsonValueIndexWritesForCell(alloc, writes, owned_keys, doc_key, cell);
    try appendJsonPathIndexWritesForCell(alloc, writes, owned_keys, doc_key, cell);
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

    try appendArrayElementIndexDeletesForCell(alloc, deletes, owned_keys, doc_key, cell);
    try appendArrayValueIndexDeleteForCell(alloc, deletes, owned_keys, doc_key, cell);
    try appendJsonValueIndexDeletesForCell(alloc, deletes, owned_keys, doc_key, cell);
    try appendJsonPathIndexDeletesForCell(alloc, deletes, owned_keys, doc_key, cell);
}

fn appendArrayElementIndexWritesForCell(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .array) return;

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (parsed.value.array.items) |item| {
        const element_key = try arrayElementIndexKeyForValueAlloc(alloc, item);
        var element_key_owned = true;
        errdefer if (element_key_owned) alloc.free(element_key);
        const gop = try seen.getOrPut(alloc, element_key);
        if (gop.found_existing) {
            alloc.free(element_key);
            element_key_owned = false;
            continue;
        }
        gop.key_ptr.* = element_key;
        element_key_owned = false;

        const key = try internal_keys.relationalArrayElementIndexKeyAlloc(alloc, cell.path, element_key, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try writes.append(alloc, .{ .key = key, .value = "" });
        key_owned = false;
    }

    var it = seen.keyIterator();
    while (it.next()) |key| alloc.free(key.*);
}

fn appendArrayValueIndexWriteForCell(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .array) return;
    const array_key = try arrayValueIndexKeyForValueAlloc(alloc, parsed.value);
    defer alloc.free(array_key);
    const key = try internal_keys.relationalArrayValueIndexKeyAlloc(alloc, cell.path, array_key, doc_key);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (owned_keys) |keys| {
        try keys.append(alloc, key);
        key_owned = false;
    }
    try writes.append(alloc, .{ .key = key, .value = "" });
    key_owned = false;
}

fn appendArrayElementIndexDeletesForCell(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .array) return;

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (parsed.value.array.items) |item| {
        const element_key = try arrayElementIndexKeyForValueAlloc(alloc, item);
        var element_key_owned = true;
        errdefer if (element_key_owned) alloc.free(element_key);
        const gop = try seen.getOrPut(alloc, element_key);
        if (gop.found_existing) {
            alloc.free(element_key);
            element_key_owned = false;
            continue;
        }
        gop.key_ptr.* = element_key;
        element_key_owned = false;

        const key = try internal_keys.relationalArrayElementIndexKeyAlloc(alloc, cell.path, element_key, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try deletes.append(alloc, key);
        key_owned = false;
    }

    var it = seen.keyIterator();
    while (it.next()) |key| alloc.free(key.*);
}

fn appendArrayValueIndexDeleteForCell(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .array) return;
    const array_key = try arrayValueIndexKeyForValueAlloc(alloc, parsed.value);
    defer alloc.free(array_key);
    const key = try internal_keys.relationalArrayValueIndexKeyAlloc(alloc, cell.path, array_key, doc_key);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    if (owned_keys) |keys| {
        try keys.append(alloc, key);
        key_owned = false;
    }
    try deletes.append(alloc, key);
    key_owned = false;
}

const JsonValueIndexEntry = struct {
    json_path: []u8,
    value_key: []u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.json_path);
        alloc.free(self.value_key);
        self.* = undefined;
    }
};

fn appendJsonValueIndexWritesForCell(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();

    var entries = std.ArrayListUnmanaged(JsonValueIndexEntry).empty;
    defer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }
    try collectJsonValueIndexEntriesAlloc(alloc, "", parsed.value, &entries);
    for (entries.items) |entry| {
        const key = try internal_keys.relationalJsonValueIndexKeyAlloc(alloc, cell.path, entry.json_path, entry.value_key, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try writes.append(alloc, .{ .key = key, .value = "" });
        key_owned = false;
    }
}

fn appendJsonValueIndexDeletesForCell(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();

    var entries = std.ArrayListUnmanaged(JsonValueIndexEntry).empty;
    defer {
        for (entries.items) |*entry| entry.deinit(alloc);
        entries.deinit(alloc);
    }
    try collectJsonValueIndexEntriesAlloc(alloc, "", parsed.value, &entries);
    for (entries.items) |entry| {
        const key = try internal_keys.relationalJsonValueIndexKeyAlloc(alloc, cell.path, entry.json_path, entry.value_key, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try deletes.append(alloc, key);
        key_owned = false;
    }
}

fn appendJsonPathIndexWritesForCell(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();

    var paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (paths.items) |path| alloc.free(path);
        paths.deinit(alloc);
    }
    try collectJsonPathIndexPathsAlloc(alloc, "", parsed.value, &paths);
    for (paths.items) |path| {
        const key = try internal_keys.relationalJsonPathIndexKeyAlloc(alloc, cell.path, path, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try writes.append(alloc, .{ .key = key, .value = "" });
        key_owned = false;
    }
}

fn appendJsonPathIndexDeletesForCell(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    cell: relational_row_codec.Cell,
) !void {
    if (cell.value_type != .bytes_val or !cell.is_json) return;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, cell.value.bytes_val, .{}) catch return;
    defer parsed.deinit();

    var paths = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (paths.items) |path| alloc.free(path);
        paths.deinit(alloc);
    }
    try collectJsonPathIndexPathsAlloc(alloc, "", parsed.value, &paths);
    for (paths.items) |path| {
        const key = try internal_keys.relationalJsonPathIndexKeyAlloc(alloc, cell.path, path, doc_key);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        if (owned_keys) |keys| {
            try keys.append(alloc, key);
            key_owned = false;
        }
        try deletes.append(alloc, key);
        key_owned = false;
    }
}

fn collectJsonValueIndexEntriesAlloc(
    alloc: Allocator,
    path: []const u8,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(JsonValueIndexEntry),
) !void {
    switch (value) {
        .object => |object| {
            for (object.keys(), object.values()) |key, child| {
                const child_path = try joinJsonIndexPathAlloc(alloc, path, key);
                defer alloc.free(child_path);
                try collectJsonValueIndexEntriesAlloc(alloc, child_path, child, out);
            }
        },
        .array => |array| {
            for (array.items) |child| {
                try collectJsonValueIndexEntriesAlloc(alloc, path, child, out);
            }
        },
        else => {
            const value_key = try jsonValueIndexKeyForValueAlloc(alloc, value);
            var value_key_owned = true;
            errdefer if (value_key_owned) alloc.free(value_key);
            const json_path = try alloc.dupe(u8, path);
            var json_path_owned = true;
            errdefer if (json_path_owned) alloc.free(json_path);
            for (out.items) |entry| {
                if (std.mem.eql(u8, entry.json_path, json_path) and std.mem.eql(u8, entry.value_key, value_key)) {
                    alloc.free(json_path);
                    json_path_owned = false;
                    alloc.free(value_key);
                    value_key_owned = false;
                    return;
                }
            }
            try out.append(alloc, .{
                .json_path = json_path,
                .value_key = value_key,
            });
            json_path_owned = false;
            value_key_owned = false;
        },
    }
}

fn collectJsonPathIndexPathsAlloc(
    alloc: Allocator,
    path: []const u8,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged([]u8),
) !void {
    if (path.len > 0) try appendUniqueJsonPathIndexPathAlloc(alloc, path, out);
    switch (value) {
        .object => |object| {
            for (object.keys(), object.values()) |key, child| {
                const child_path = try joinJsonIndexPathAlloc(alloc, path, key);
                defer alloc.free(child_path);
                try collectJsonPathIndexPathsAlloc(alloc, child_path, child, out);
            }
        },
        .array => |array| {
            for (array.items) |child| {
                try collectJsonPathIndexPathsAlloc(alloc, path, child, out);
            }
        },
        else => {},
    }
}

fn appendUniqueJsonPathIndexPathAlloc(
    alloc: Allocator,
    path: []const u8,
    out: *std.ArrayListUnmanaged([]u8),
) !void {
    for (out.items) |existing| {
        if (std.mem.eql(u8, existing, path)) return;
    }
    try out.append(alloc, try alloc.dupe(u8, path));
}

fn joinJsonIndexPathAlloc(alloc: Allocator, prefix: []const u8, segment: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, segment);
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ prefix, segment });
}

fn appendDeleteInternal(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    column_index_policy: ColumnIndexPolicy,
) !void {
    const old_row = try getRawAlloc(alloc, store, doc_key);
    defer if (old_row) |row| alloc.free(row);
    var old_decoded: relational_row_codec.Row = undefined;
    var old_decoded_ready = false;
    defer if (old_decoded_ready) old_decoded.deinit(alloc);
    if (old_row != null and column_index_policy.mayIndexAnyRow()) {
        old_decoded = try relational_row_codec.deserialize(alloc, old_row.?);
        old_decoded_ready = true;
    }
    try appendDeleteInternalWithOldRowAndCells(alloc, deletes, owned_keys, doc_key, old_row, if (old_decoded_ready) old_decoded.cells else null, column_index_policy);
}

fn appendDeleteInternalWithOldRowAndCells(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row_value: ?[]const u8,
    old_cells: ?[]const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
) !void {
    if (column_index_policy.mayIndexAnyRow()) try validateColumnIndexPolicyForWriteMaintenance(column_index_policy);

    if (old_row_value) |row| {
        if (column_index_policy.mayIndexAnyRow()) {
            try appendExistingColumnDeletesForRow(alloc, deletes, owned_keys, doc_key, row, old_cells orelse return error.InvalidArgument, column_index_policy);
        }
    }

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
    column_index_policy: ColumnIndexPolicy,
) !void {
    const old_row = try getRawAlloc(alloc, store, doc_key) orelse return;
    defer alloc.free(old_row);
    var row = try relational_row_codec.deserialize(alloc, old_row);
    defer row.deinit(alloc);
    try appendExistingColumnDeletesForRow(alloc, deletes, owned_keys, doc_key, old_row, row.cells, column_index_policy);
}

fn appendExistingColumnDeletesForRow(
    alloc: Allocator,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: ?*std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    old_row: []const u8,
    cells: []const relational_row_codec.Cell,
    column_index_policy: ColumnIndexPolicy,
) !void {
    const may_index_any_row = column_index_policy.mayIndexAnyRow();
    for (cells) |cell| {
        if (may_index_any_row and try column_index_policy.shouldMaintainScalarIndexRow(alloc, cell.path, old_row)) {
            try appendColumnIndexDeleteForCell(alloc, deletes, owned_keys, doc_key, cell);
        }
    }
    if (may_index_any_row) {
        try appendOrderedTupleIndexDeletesForRowWithColumnIndexPolicy(alloc, deletes, owned_keys, doc_key, old_row, cells, column_index_policy);
    }
}

fn cloneTypedValue(alloc: Allocator, value_type: typed_dv.ValueType, value: typed_dv.TypedValue) !typed_dv.TypedValue {
    return switch (value_type) {
        .u64_val => .{ .u64_val = value.u64_val },
        .i64_val => .{ .i64_val = value.i64_val },
        .f64_val => .{ .f64_val = value.f64_val },
        .bool_val => .{ .bool_val = value.bool_val },
        .geo_point => .{ .geo_point = value.geo_point },
        .bytes_val => .{ .bytes_val = try alloc.dupe(u8, value.bytes_val) },
    };
}

const RelationalStoreCountingAllocator = struct {
    backing: Allocator,
    allocated_total: usize = 0,
    live_bytes: usize = 0,

    fn init(backing: Allocator) RelationalStoreCountingAllocator {
        return .{ .backing = backing };
    }

    fn reset(self: *@This()) void {
        self.allocated_total = 0;
        self.live_bytes = 0;
    }

    fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn recordAlloc(self: *@This(), len: usize) void {
        self.allocated_total += len;
        self.live_bytes += len;
    }

    fn recordFree(self: *@This(), len: usize) void {
        self.live_bytes -|= len;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.recordAlloc(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.recordFree(memory.len);
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    const vtable = Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

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

test "relational ordered tuple index entry keys encode row values for ordered scans" {
    const alloc = std.testing.allocator;
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword, .nullable = true },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime, .nullable = false },
    };
    const asc_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const desc_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "amount", .direction = .desc },
    };
    const nulls_first_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status", .nulls = .first },
    };
    const nulls_last_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status", .nulls = .last },
    };

    const low_cells = [_]relational_row_codec.Cell{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = -3.5 } },
        .{ .path = "created_at", .value_type = .u64_val, .value = .{ .u64_val = 100 } },
    };
    const high_cells = [_]relational_row_codec.Cell{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 42.0 } },
        .{ .path = "created_at", .value_type = .u64_val, .value = .{ .u64_val = 200 } },
    };
    const null_status_cells = [_]relational_row_codec.Cell{
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 0.0 } },
        .{ .path = "created_at", .value_type = .u64_val, .value = .{ .u64_val = 300 } },
    };

    const low_row = try relational_row_codec.serialize(alloc, &low_cells);
    defer alloc.free(low_row);
    const high_row = try relational_row_codec.serialize(alloc, &high_cells);
    defer alloc.free(high_row);
    const null_status_row = try relational_row_codec.serialize(alloc, &null_status_cells);
    defer alloc.free(null_status_row);

    const low_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, low_row, &asc_keys, &columns);
    defer alloc.free(low_tuple);
    const high_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, high_row, &asc_keys, &columns);
    defer alloc.free(high_tuple);
    try std.testing.expect(std.mem.order(u8, low_tuple, high_tuple) == .lt);

    const low_desc_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, low_row, &desc_keys, &columns);
    defer alloc.free(low_desc_tuple);
    const high_desc_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, high_row, &desc_keys, &columns);
    defer alloc.free(high_desc_tuple);
    try std.testing.expect(std.mem.order(u8, high_desc_tuple, low_desc_tuple) == .lt);

    const null_first_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, null_status_row, &nulls_first_keys, &columns);
    defer alloc.free(null_first_tuple);
    const present_first_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, low_row, &nulls_first_keys, &columns);
    defer alloc.free(present_first_tuple);
    try std.testing.expect(std.mem.order(u8, null_first_tuple, present_first_tuple) == .lt);

    const null_last_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, null_status_row, &nulls_last_keys, &columns);
    defer alloc.free(null_last_tuple);
    const present_last_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, low_row, &nulls_last_keys, &columns);
    defer alloc.free(present_last_tuple);
    try std.testing.expect(std.mem.order(u8, present_last_tuple, null_last_tuple) == .lt);

    var entry_keys = try orderedTupleIndexEntryKeysForRowAlloc(alloc, "orders_status_amount_idx", "doc:a", low_row, &asc_keys, &columns);
    defer entry_keys.deinit(alloc);
    var decoded_forward = (try internal_keys.decodeRelationalOrderedTupleIndexKeyAlloc(alloc, entry_keys.forward_key)).?;
    defer decoded_forward.deinit(alloc);
    try std.testing.expectEqualStrings("orders_status_amount_idx", decoded_forward.index_id);
    try std.testing.expectEqualStrings("doc:a", decoded_forward.doc_key);
    try std.testing.expectEqualSlices(u8, entry_keys.encoded_tuple, decoded_forward.encoded_tuple);
    var decoded_reverse = (try internal_keys.decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc, entry_keys.reverse_key)).?;
    defer decoded_reverse.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", decoded_reverse.doc_key);
    try std.testing.expectEqualStrings("orders_status_amount_idx", decoded_reverse.index_id);
    try std.testing.expectEqualSlices(u8, entry_keys.encoded_tuple, decoded_reverse.encoded_tuple);
}

test "relational ordered tuple staging transfers owned buffers without allocation" {
    var counting = RelationalStoreCountingAllocator.init(std.testing.allocator);
    const alloc = counting.allocator();
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const cells = [_]relational_row_codec.Cell{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    };

    var keys = try orderedTupleIndexEntryKeysForCellsAlloc(alloc, "orders_status_amount_idx", "doc:a", cells[0..], index_keys[0..], &.{"note"}, columns[0..]);
    defer keys.deinit(alloc);

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

    try writes.ensureUnusedCapacity(alloc, 2);
    try owned_keys.ensureUnusedCapacity(alloc, 2);
    try owned_values.ensureUnusedCapacity(alloc, 1);
    counting.reset();

    try appendOrderedTupleIndexWriteKeys(alloc, &writes, &owned_keys, &owned_values, &keys);

    try std.testing.expectEqual(@as(usize, 0), counting.allocated_total);
    try std.testing.expectEqual(@as(usize, 2), writes.items.len);
    try std.testing.expectEqual(@as(usize, 2), owned_keys.items.len);
    try std.testing.expectEqual(@as(usize, 1), owned_values.items.len);
    try std.testing.expectEqual(@as(usize, 0), keys.forward_key.len);
    try std.testing.expectEqual(@as(usize, 0), keys.reverse_key.len);
    try std.testing.expectEqual(@as(usize, 0), keys.forward_value.len);
    try std.testing.expect(keys.encoded_tuple.len > 0);
}

test "relational ordered tuple index maintenance skips unchanged overwrites" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true },
        .{ .name = "note", .path = "note", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .include_columns = &.{"note"},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};

    const initial_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(initial_row);
    const payload_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(payload_changed_row);
    const payload_removed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
    });
    defer alloc.free(payload_removed_row);
    const changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(changed_row);

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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const status_prefix_keys = [_]schema_mod.RelationalIndexKey{.{ .column = "status" }};
    const active_prefix = try orderedTupleValueForIndexKeysAlloc(alloc, initial_row, &status_prefix_keys, &columns);
    defer alloc.free(active_prefix);
    const initial_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, initial_row, indexes[0].keys, &columns);
    defer alloc.free(initial_tuple);
    const changed_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, changed_row, indexes[0].keys, &columns);
    defer alloc.free(changed_tuple);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", initial_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, active_docs);
    try std.testing.expectEqual(@as(usize, 1), active_docs.len);
    try std.testing.expectEqualStrings("doc:a", active_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", initial_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", payload_changed_row, policy);
    try std.testing.expectEqual(@as(usize, 1), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const unchanged_tuple_forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", initial_tuple, "doc:a");
    defer alloc.free(unchanged_tuple_forward_key);
    const payload_value = try store.get(alloc, unchanged_tuple_forward_key);
    defer alloc.free(payload_value);
    const payload_json = try relational_row_codec.reconstructValueAlloc(alloc, payload_value);
    defer alloc.free(payload_json);
    try std.testing.expectEqualStrings("{\"note\":\"beta\"}", payload_json);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", payload_removed_row, policy);
    try std.testing.expectEqual(@as(usize, 1), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const removed_payload_value = try store.get(alloc, unchanged_tuple_forward_key);
    defer alloc.free(removed_payload_value);
    const removed_payload_json = try relational_row_codec.reconstructValueAlloc(alloc, removed_payload_value);
    defer alloc.free(removed_payload_json);
    try std.testing.expectEqualStrings("{}", removed_payload_json);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", changed_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const old_tuple_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", initial_tuple, "", "");
    defer freeDocKeys(alloc, old_tuple_docs);
    try std.testing.expectEqual(@as(usize, 0), old_tuple_docs.len);
    const changed_tuple_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", changed_tuple, "", "");
    defer freeDocKeys(alloc, changed_tuple_docs);
    try std.testing.expectEqual(@as(usize, 1), changed_tuple_docs.len);
    try std.testing.expectEqualStrings("doc:a", changed_tuple_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:a", policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_after_delete = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, active_after_delete);
    try std.testing.expectEqual(@as(usize, 0), active_after_delete.len);
}

test "relational ordered tuple column maintenance fails closed on missing durable identity" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
    });
    defer alloc.free(row);

    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
    };
    const missing_generation_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
    }};
    const missing_fingerprint_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .generation = 7,
    }};

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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:missing-generation", row, ColumnIndexPolicy.fromSchemaParts(columns[0..], missing_generation_indexes[0..]));
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:missing-fingerprint", row, ColumnIndexPolicy.fromSchemaParts(columns[0..], missing_fingerprint_indexes[0..]));
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
}

test "relational ordered tuple index maintenance tracks partial predicate membership" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true },
        .{ .name = "note", .path = "note", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_partial_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_partial_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};

    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(inactive_row);
    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(active_row);
    const active_note_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(active_note_changed_row);
    const active_amount_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(active_amount_changed_row);

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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const active_prefix_keys = [_]schema_mod.RelationalIndexKey{.{ .column = "status" }};
    const active_prefix = try orderedTupleValueForIndexKeysAlloc(alloc, active_row, &active_prefix_keys, &columns);
    defer alloc.free(active_prefix);
    const initial_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, active_row, indexes[0].keys, &columns);
    defer alloc.free(initial_tuple);
    const changed_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, active_amount_changed_row, indexes[0].keys, &columns);
    defer alloc.free(changed_tuple);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", inactive_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, inactive_docs);
    try std.testing.expectEqual(@as(usize, 0), inactive_docs.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, active_docs);
    try std.testing.expectEqual(@as(usize, 1), active_docs.len);
    try std.testing.expectEqualStrings("doc:a", active_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_note_changed_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_amount_changed_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const old_tuple_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", initial_tuple, "", "");
    defer freeDocKeys(alloc, old_tuple_docs);
    try std.testing.expectEqual(@as(usize, 0), old_tuple_docs.len);
    const changed_tuple_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", changed_tuple, "", "");
    defer freeDocKeys(alloc, changed_tuple_docs);
    try std.testing.expectEqual(@as(usize, 1), changed_tuple_docs.len);
    try std.testing.expectEqualStrings("doc:a", changed_tuple_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", inactive_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_again_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, inactive_again_docs);
    try std.testing.expectEqual(@as(usize, 0), inactive_again_docs.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_row, policy);
    try store.putBatch(writes.items, deletes.items);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:a", policy);
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const deleted_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_partial_idx", active_prefix, "", "");
    defer freeDocKeys(alloc, deleted_docs);
    try std.testing.expectEqual(@as(usize, 0), deleted_docs.len);
}

test "relational ordered tuple index scan orders duplicate tuples by document key" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true },
        .{ .name = "note", .path = "note", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_dup_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_dup_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};

    const duplicate_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "same tuple" } },
    });
    defer alloc.free(duplicate_row);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:c", duplicate_row, policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", duplicate_row, policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", duplicate_row, policy);
    try store.putBatch(writes.items, deletes.items);

    const duplicate_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, duplicate_row, indexes[0].keys, &columns);
    defer alloc.free(duplicate_tuple);
    const duplicate_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_dup_idx", duplicate_tuple, "", "");
    defer freeDocKeys(alloc, duplicate_docs);
    try std.testing.expectEqual(@as(usize, 3), duplicate_docs.len);
    try std.testing.expectEqualStrings("doc:a", duplicate_docs[0]);
    try std.testing.expectEqualStrings("doc:b", duplicate_docs[1]);
    try std.testing.expectEqualStrings("doc:c", duplicate_docs[2]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:b", policy);
    try store.putBatch(writes.items, deletes.items);

    const after_delete_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_dup_idx", duplicate_tuple, "", "");
    defer freeDocKeys(alloc, after_delete_docs);
    try std.testing.expectEqual(@as(usize, 2), after_delete_docs.len);
    try std.testing.expectEqualStrings("doc:a", after_delete_docs[0]);
    try std.testing.expectEqualStrings("doc:c", after_delete_docs[1]);
}

test "relational ordered tuple index maintenance respects lifecycle states" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const ready_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const building_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const invalid_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const dropping_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const catching_up_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const stale_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const rebuild_required_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const failed_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const missing_record_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const stale_record_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "ready_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "building_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "invalid_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "dropping_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "catching_up_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "stale_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "rebuild_required_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "failed_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "missing_record_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "stale_record_status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "orders_ready_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "ready_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = ready_keys[0..],
            .lifecycle = .ready,
            .generation = 10,
            .schema_fingerprint = "secondary-index-v1:orders_ready_tuple_idx",
            .generation_record = .{ .generation = 10, .lifecycle = .ready },
        },
        .{
            .name = "orders_building_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "building_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = building_keys[0..],
            .lifecycle = .building,
            .generation = 11,
            .schema_fingerprint = "secondary-index-v1:orders_building_tuple_idx",
            .generation_record = .{ .generation = 11, .lifecycle = .building },
        },
        .{
            .name = "orders_invalid_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "invalid_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = invalid_keys[0..],
            .lifecycle = .invalid,
            .generation = 12,
            .schema_fingerprint = "secondary-index-v1:orders_invalid_tuple_idx",
            .generation_record = .{ .generation = 12, .lifecycle = .invalid },
        },
        .{
            .name = "orders_dropping_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "dropping_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = dropping_keys[0..],
            .lifecycle = .dropping,
            .generation = 13,
            .schema_fingerprint = "secondary-index-v1:orders_dropping_tuple_idx",
            .generation_record = .{ .generation = 13, .lifecycle = .dropping },
        },
        .{
            .name = "orders_catching_up_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "catching_up_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = catching_up_keys[0..],
            .lifecycle = .catching_up,
            .generation = 14,
            .schema_fingerprint = "secondary-index-v1:orders_catching_up_tuple_idx",
            .generation_record = .{ .generation = 14, .lifecycle = .catching_up },
        },
        .{
            .name = "orders_stale_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "stale_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = stale_keys[0..],
            .lifecycle = .stale,
            .generation = 15,
            .schema_fingerprint = "secondary-index-v1:orders_stale_tuple_idx",
            .generation_record = .{ .generation = 15, .lifecycle = .stale },
        },
        .{
            .name = "orders_rebuild_required_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "rebuild_required_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = rebuild_required_keys[0..],
            .lifecycle = .rebuild_required,
            .generation = 16,
            .schema_fingerprint = "secondary-index-v1:orders_rebuild_required_tuple_idx",
            .generation_record = .{ .generation = 16, .lifecycle = .rebuild_required },
        },
        .{
            .name = "orders_failed_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "failed_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = failed_keys[0..],
            .lifecycle = .failed,
            .generation = 17,
            .schema_fingerprint = "secondary-index-v1:orders_failed_tuple_idx",
            .generation_record = .{ .generation = 17, .lifecycle = .failed },
        },
        .{
            .name = "orders_missing_record_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "missing_record_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = missing_record_keys[0..],
            .lifecycle = .ready,
            .generation = 18,
            .schema_fingerprint = "secondary-index-v1:orders_missing_record_tuple_idx",
            .generation_record = null,
        },
        .{
            .name = "orders_stale_record_tuple_idx",
            .owner_kind = .relational_column,
            .owner_name = "stale_record_status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = stale_record_keys[0..],
            .lifecycle = .ready,
            .generation = 19,
            .schema_fingerprint = "secondary-index-v1:orders_stale_record_tuple_idx",
            .generation_record = .{ .generation = 18, .lifecycle = .ready },
        },
    };

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const tuple = try orderedTupleValueForIndexKeysAlloc(alloc, row, ready_keys[0..], &columns);
    defer alloc.free(tuple);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row, policy);
    try std.testing.expectEqual(@as(usize, 6), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    const ready_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_ready_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, ready_docs);
    try std.testing.expectEqual(@as(usize, 1), ready_docs.len);
    try std.testing.expectEqualStrings("doc:a", ready_docs[0]);

    const building_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_building_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, building_docs);
    try std.testing.expectEqual(@as(usize, 1), building_docs.len);
    try std.testing.expectEqualStrings("doc:a", building_docs[0]);

    const catching_up_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_catching_up_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, catching_up_docs);
    try std.testing.expectEqual(@as(usize, 1), catching_up_docs.len);
    try std.testing.expectEqualStrings("doc:a", catching_up_docs[0]);

    const invalid_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_invalid_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, invalid_docs);
    try std.testing.expectEqual(@as(usize, 0), invalid_docs.len);

    const dropping_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_dropping_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, dropping_docs);
    try std.testing.expectEqual(@as(usize, 0), dropping_docs.len);

    const stale_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_stale_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, stale_docs);
    try std.testing.expectEqual(@as(usize, 0), stale_docs.len);

    const rebuild_required_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_rebuild_required_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, rebuild_required_docs);
    try std.testing.expectEqual(@as(usize, 0), rebuild_required_docs.len);

    const failed_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_failed_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, failed_docs);
    try std.testing.expectEqual(@as(usize, 0), failed_docs.len);
    const missing_record_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_missing_record_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, missing_record_docs);
    try std.testing.expectEqual(@as(usize, 0), missing_record_docs.len);
    const stale_record_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_stale_record_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, stale_record_docs);
    try std.testing.expectEqual(@as(usize, 0), stale_record_docs.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:a", policy);
    try std.testing.expectEqual(@as(usize, 6), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    const ready_after_delete = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_ready_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, ready_after_delete);
    try std.testing.expectEqual(@as(usize, 0), ready_after_delete.len);

    const building_after_delete = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_building_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, building_after_delete);
    try std.testing.expectEqual(@as(usize, 0), building_after_delete.len);

    const catching_up_after_delete = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_catching_up_tuple_idx", tuple, "", "");
    defer freeDocKeys(alloc, catching_up_after_delete);
    try std.testing.expectEqual(@as(usize, 0), catching_up_after_delete.len);
}

test "relational ordered tuple index mutation batches are retry idempotent" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
        .{ .name = "note", .path = "note", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_retry_tuple_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .include_columns = &.{"note"},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_retry_tuple_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};

    const initial_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(initial_row);
    const changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(changed_row);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const initial_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, initial_row, indexes[0].keys, &columns);
    defer alloc.free(initial_tuple);
    const changed_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, changed_row, indexes[0].keys, &columns);
    defer alloc.free(changed_tuple);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", initial_row, policy);
    try store.putBatch(writes.items, deletes.items);
    try store.putBatch(writes.items, deletes.items);
    const initial_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_retry_tuple_idx", initial_tuple, "", "");
    defer freeDocKeys(alloc, initial_docs);
    try std.testing.expectEqual(@as(usize, 1), initial_docs.len);
    try std.testing.expectEqualStrings("doc:a", initial_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", changed_row, policy);
    try store.putBatch(writes.items, deletes.items);
    try store.putBatch(writes.items, deletes.items);

    const stale_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_retry_tuple_idx", initial_tuple, "", "");
    defer freeDocKeys(alloc, stale_docs);
    try std.testing.expectEqual(@as(usize, 0), stale_docs.len);
    const changed_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_retry_tuple_idx", changed_tuple, "", "");
    defer freeDocKeys(alloc, changed_docs);
    try std.testing.expectEqual(@as(usize, 1), changed_docs.len);
    try std.testing.expectEqualStrings("doc:a", changed_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:a", policy);
    try store.putBatch(writes.items, deletes.items);
    try store.putBatch(writes.items, deletes.items);
    const deleted_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_retry_tuple_idx", changed_tuple, "", "");
    defer freeDocKeys(alloc, deleted_docs);
    try std.testing.expectEqual(@as(usize, 0), deleted_docs.len);
}

test "relational index reserve counts catalog indexes instead of all cells" {
    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
        .{ .name = "customer", .path = "customer", .field_type = .keyword, .indexed = false },
        .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = false },
        .{ .name = "note", .path = "note", .field_type = .keyword, .indexed = false },
    };
    const tuple_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "orders_status_amount_idx",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .keys = tuple_keys[0..],
            .lifecycle = .ready,
            .generation = 7,
            .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
            .generation_record = .{ .generation = 7, .lifecycle = .ready },
        },
        .{
            .name = "customer",
            .owner_kind = .relational_column,
            .owner_name = "customer",
            .access_method = .scalar_column,
            .columns = &.{"customer"},
            .lifecycle = .ready,
            .generation = 7,
            .schema_fingerprint = "secondary-index-v1:customer",
        },
        .{
            .name = "orders_metadata_gin",
            .owner_kind = .relational_column,
            .owner_name = "metadata",
            .access_method = .algebraic_filter,
            .columns = &.{"metadata"},
            .lifecycle = .ready,
            .generation = 7,
            .schema_fingerprint = "secondary-index-v1:orders_metadata_gin",
        },
    };
    const cells = [_]relational_row_codec.Cell{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "customer", .value_type = .bytes_val, .value = .{ .bytes_val = "c1" } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "wide payload" } },
    };

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const catalog_counts = insertIndexReserveCounts(policy, cells[0..]);
    try std.testing.expectEqual(@as(usize, 8), catalog_counts.writes);
    try std.testing.expectEqual(@as(usize, 7), catalog_counts.keys);
    try std.testing.expectEqual(@as(usize, 2), catalog_counts.values);
    try std.testing.expect(!catalogCellHasScalarIndex(policy, "metadata"));

    const old_cells = cells[0..3];
    const changed_counts = changedScalarIndexReserveCounts(policy, old_cells, cells[0..]);
    try std.testing.expectEqual(@as(usize, 6), changed_counts.writes);
    try std.testing.expectEqual(@as(usize, 5), changed_counts.deletes);
    try std.testing.expectEqual(@as(usize, 10), changed_counts.keys);
    try std.testing.expectEqual(@as(usize, 1), changed_counts.values);
}

test "relational scalar column index maintenance skips unchanged and tracks predicate membership" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{
            .name = "status",
            .path = "status",
            .field_type = .keyword,
            .indexed = false,
        },
        .{
            .name = "amount",
            .path = "amount",
            .field_type = .numeric,
            .indexed = false,
        },
        .{
            .name = "note",
            .path = "note",
            .field_type = .keyword,
            .indexed = false,
        },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "amount",
        .owner_kind = .relational_column,
        .owner_name = "amount",
        .access_method = .scalar_column,
        .columns = &.{"amount"},
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:amount",
    }};

    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(inactive_row);
    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "alpha" } },
    });
    defer alloc.free(active_row);
    const active_note_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 10.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(active_note_changed_row);
    const active_amount_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 20.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "beta" } },
    });
    defer alloc.free(active_amount_changed_row);

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

    const countScalarWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalColumnIndexKey(item.key) or
                    internal_keys.isRelationalColumnIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countScalarDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalColumnIndexKey(key) or
                    internal_keys.isRelationalColumnIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", inactive_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countScalarWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_values = try scanColumnAlloc(alloc, &store, "amount", "", "");
    defer freeColumnValues(alloc, inactive_values);
    try std.testing.expectEqual(@as(usize, 0), inactive_values.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countScalarWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_values = try scanColumnAlloc(alloc, &store, "amount", "", "");
    defer freeColumnValues(alloc, active_values);
    try std.testing.expectEqual(@as(usize, 1), active_values.len);
    try std.testing.expectEqualStrings("doc:a", active_values[0].doc_key);
    try std.testing.expectEqual(@as(f64, 10.0), active_values[0].value.f64_val);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_note_changed_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countScalarWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_amount_changed_row, policy);
    try std.testing.expectEqual(@as(usize, 2), countScalarWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const changed_values = try scanColumnAlloc(alloc, &store, "amount", "", "");
    defer freeColumnValues(alloc, changed_values);
    try std.testing.expectEqual(@as(usize, 1), changed_values.len);
    try std.testing.expectEqual(@as(f64, 20.0), changed_values[0].value.f64_val);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", inactive_row, policy);
    try std.testing.expectEqual(@as(usize, 0), countScalarWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_again_values = try scanColumnAlloc(alloc, &store, "amount", "", "");
    defer freeColumnValues(alloc, inactive_again_values);
    try std.testing.expectEqual(@as(usize, 0), inactive_again_values.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", active_row, policy);
    try store.putBatch(writes.items, deletes.items);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDeleteWithColumnIndexPolicy(alloc, &store, &deletes, &owned_keys, "doc:a", policy);
    try std.testing.expectEqual(@as(usize, 2), countScalarDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const deleted_values = try scanColumnAlloc(alloc, &store, "amount", "", "");
    defer freeColumnValues(alloc, deleted_values);
    try std.testing.expectEqual(@as(usize, 0), deleted_values.len);
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

test "relational unique constraints optionally treat null components as not distinct" {
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
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "beta" },
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

    const default_unique = [_]schema_mod.UniqueConstraint{.{
        .name = "events_email_key",
        .columns = &.{"email"},
    }};
    var default_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    default_participant.configureUniqueConstraints(&default_unique);
    try default_participant.prepareUpsert("events", "doc:a", row_a, null);
    try default_participant.prepareUpsert("events", "doc:b", row_b, null);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    const nulls_not_distinct_unique = [_]schema_mod.UniqueConstraint{.{
        .name = "events_email_key",
        .columns = &.{"email"},
        .nulls_not_distinct = true,
    }};
    var strict_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    strict_participant.configureUniqueConstraints(&nulls_not_distinct_unique);
    try strict_participant.prepareUpsert("events", "doc:c", row_a, null);
    try std.testing.expectError(
        error.UniqueConstraintViolation,
        strict_participant.prepareUpsert("events", "doc:d", row_b, null),
    );
}

test "relational unique constraints honor case-insensitive column collation" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "Alice@Example.test" },
        },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alice@example.test" },
        },
    });
    defer alloc.free(row_b);

    const columns = [_]schema_mod.RelationalColumn{.{
        .name = "email",
        .path = "email",
        .field_type = .keyword,
        .collation = "antfly.case_insensitive",
    }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "events_email_key",
        .columns = &.{"email"},
    }};
    const index_keys = [_]schema_mod.RelationalIndexKey{.{ .column = "email" }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "events_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "events_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{"email"},
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 1,
        .schema_fingerprint = "unique-index-v1:events_email_key",
        .generation_record = .{ .generation = 1, .lifecycle = .ready },
    }};

    const tuple_a = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_a, unique[0], &columns)) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple_a);
    const tuple_b = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_b, unique[0], &columns)) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple_b);
    try std.testing.expectEqualSlices(u8, tuple_a, tuple_b);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configurePeriods(&.{}, &columns);
    try participant.prepareUpsert("events", "doc:a", row_a, null);
    try std.testing.expectError(
        error.UniqueConstraintViolation,
        participant.prepareUpsert("events", "doc:b", row_b, null),
    );
}

test "relational ordered tuple entries track unique constraint metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        .{ .name = "display_name", .path = "display_name", .field_type = .keyword, .indexed = false },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .include_columns = &.{"display_name"},
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "unique-index-v1:users_tenant_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};

    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
        .{ .path = "display_name", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada" } },
    });
    defer alloc.free(inactive_row);
    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "display_name", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada" } },
    });
    defer alloc.free(active_row);
    const payload_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "display_name", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada Lovelace" } },
    });
    defer alloc.free(payload_changed_row);
    const key_changed_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada+2@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "display_name", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada Lovelace" } },
    });
    defer alloc.free(key_changed_row);

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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const active_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, active_row, indexes[0].keys, columns[0..]);
    defer alloc.free(active_tuple);
    const changed_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, key_changed_row, indexes[0].keys, columns[0..]);
    defer alloc.free(changed_tuple);

    var inactive_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    inactive_participant.configureUniqueConstraints(&unique);
    inactive_participant.configurePeriods(&.{}, columns[0..]);
    try inactive_participant.prepareUpsert("users", "doc:a", inactive_row, null);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", active_tuple, "", "");
    defer freeDocKeys(alloc, inactive_docs);
    try std.testing.expectEqual(@as(usize, 0), inactive_docs.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var active_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    active_participant.configureUniqueConstraints(&unique);
    active_participant.configurePeriods(&.{}, columns[0..]);
    try active_participant.prepareUpsert("users", "doc:a", active_row, null);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", active_tuple, "", "");
    defer freeDocKeys(alloc, active_docs);
    try std.testing.expectEqual(@as(usize, 1), active_docs.len);
    try std.testing.expectEqualStrings("doc:a", active_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var payload_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    payload_participant.configureUniqueConstraints(&unique);
    payload_participant.configurePeriods(&.{}, columns[0..]);
    try payload_participant.prepareUpsert("users", "doc:a", payload_changed_row, null);
    try std.testing.expectEqual(@as(usize, 1), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const active_forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "users_tenant_email_key", active_tuple, "doc:a");
    defer alloc.free(active_forward_key);
    const payload_value = try store.get(alloc, active_forward_key);
    defer alloc.free(payload_value);
    const payload_json = try relational_row_codec.reconstructValueAlloc(alloc, payload_value);
    defer alloc.free(payload_json);
    try std.testing.expectEqualStrings("{\"display_name\":\"Ada Lovelace\"}", payload_json);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var key_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    key_participant.configureUniqueConstraints(&unique);
    key_participant.configurePeriods(&.{}, columns[0..]);
    try key_participant.prepareUpsert("users", "doc:a", key_changed_row, null);
    try std.testing.expectEqual(@as(usize, 2), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const old_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", active_tuple, "", "");
    defer freeDocKeys(alloc, old_docs);
    try std.testing.expectEqual(@as(usize, 0), old_docs.len);
    const changed_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", changed_tuple, "", "");
    defer freeDocKeys(alloc, changed_docs);
    try std.testing.expectEqual(@as(usize, 1), changed_docs.len);
    try std.testing.expectEqualStrings("doc:a", changed_docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var inactive_again_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    inactive_again_participant.configureUniqueConstraints(&unique);
    inactive_again_participant.configurePeriods(&.{}, columns[0..]);
    try inactive_again_participant.prepareUpsert("users", "doc:a", inactive_row, null);
    try std.testing.expectEqual(@as(usize, 0), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const inactive_again_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", changed_tuple, "", "");
    defer freeDocKeys(alloc, inactive_again_docs);
    try std.testing.expectEqual(@as(usize, 0), inactive_again_docs.len);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var reinsert_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    reinsert_participant.configureUniqueConstraints(&unique);
    reinsert_participant.configurePeriods(&.{}, columns[0..]);
    try reinsert_participant.prepareUpsert("users", "doc:a", active_row, null);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_participant.configureUniqueConstraints(&unique);
    delete_participant.configurePeriods(&.{}, columns[0..]);
    try delete_participant.prepareDelete("users", "doc:a", null);
    try std.testing.expectEqual(@as(usize, 2), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
    const deleted_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", active_tuple, "", "");
    defer freeDocKeys(alloc, deleted_docs);
    try std.testing.expectEqual(@as(usize, 0), deleted_docs.len);
}

test "relational ordered tuple unique maintenance uses catalog-only index metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:users_tenant_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(row);
    const tuple = try orderedTupleValueForIndexKeysAlloc(alloc, row, indexes[0].keys, columns[0..]);
    defer alloc.free(tuple);

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

    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configurePeriods(&.{}, columns[0..]);
    try participant.prepareUpsert("users", "doc:a", row, null);
    try store.putBatch(writes.items, deletes.items);

    const docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_email_key", tuple, "", "");
    defer freeDocKeys(alloc, docs);
    try std.testing.expectEqual(@as(usize, 1), docs.len);
    try std.testing.expectEqualStrings("doc:a", docs[0]);
}

test "relational ordered tuple unique enforcement rejects missing durable identity" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const missing_generation_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .schema_fingerprint = "secondary-index-v1:users_tenant_email_key",
    }};
    const missing_fingerprint_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
    }};

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
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

    var missing_generation = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, ColumnIndexPolicy.fromSchemaParts(columns[0..], missing_generation_indexes[0..]));
    missing_generation.configureUniqueConstraints(&unique);
    missing_generation.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.RelationalIndexNotReady, missing_generation.prepareUpsert("users", "doc:missing-generation", row, null));
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    try std.testing.expectEqual(@as(usize, 0), owned_keys.items.len);
    try std.testing.expectEqual(@as(usize, 0), owned_values.items.len);

    var missing_fingerprint = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, ColumnIndexPolicy.fromSchemaParts(columns[0..], missing_fingerprint_indexes[0..]));
    missing_fingerprint.configureUniqueConstraints(&unique);
    missing_fingerprint.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.RelationalIndexNotReady, missing_fingerprint.prepareUpsert("users", "doc:missing-fingerprint", row, null));
    try std.testing.expectEqual(@as(usize, 0), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    try std.testing.expectEqual(@as(usize, 0), owned_keys.items.len);
    try std.testing.expectEqual(@as(usize, 0), owned_values.items.len);
}

test "relational ordered tuple unique maintenance uses expression catalog metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const expressions = [_]schema_mod.UniqueExpression{.{ .op = .lower, .field = "email" }};
    const index_keys = [_]schema_mod.RelationalIndexKey{.{ .column = "tenant_id" }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_lower_email_key",
        .columns = &.{"tenant_id"},
        .expressions = expressions[0..],
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_lower_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_lower_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{"tenant_id"},
        .expressions = expressions[0..],
        .keys = index_keys[0..],
        .include_columns = &.{"status"},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:users_tenant_lower_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada@Example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
    });
    defer alloc.free(row);
    const changed_case_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "verified" } },
    });
    defer alloc.free(changed_case_row);

    const tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple);
    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

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

    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configurePeriods(&.{}, columns[0..]);
    try participant.prepareUpsert("users", "doc:a", row, null);
    try store.putBatch(writes.items, deletes.items);

    const docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_tenant_lower_email_key", tuple, "", "");
    defer freeDocKeys(alloc, docs);
    try std.testing.expectEqual(@as(usize, 1), docs.len);
    try std.testing.expectEqualStrings("doc:a", docs[0]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var payload_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    payload_participant.configureUniqueConstraints(&unique);
    payload_participant.configurePeriods(&.{}, columns[0..]);
    try payload_participant.prepareUpsert("users", "doc:a", changed_case_row, null);
    try std.testing.expectEqual(@as(usize, 1), countOrderedWrites(writes.items));
    try std.testing.expectEqual(@as(usize, 0), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);

    const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "users_tenant_lower_email_key", tuple, "doc:a");
    defer alloc.free(forward_key);
    const payload = try store.get(alloc, forward_key);
    defer alloc.free(payload);
    const payload_json = try relational_row_codec.reconstructValueAlloc(alloc, payload);
    defer alloc.free(payload_json);
    try std.testing.expectEqualStrings("{\"status\":\"verified\"}", payload_json);
}

test "relational ordered tuple unique enforcement uses ready partial metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_active_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_active_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_active_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:users_active_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
    });
    defer alloc.free(inactive_row);
    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
    });
    defer alloc.free(active_row);
    const batch_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "samebatch@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
    });
    defer alloc.free(batch_row);

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

    const countDedicatedUniqueWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalUniqueKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;

    var inactive = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    inactive.configureUniqueConstraints(&unique);
    inactive.configurePeriods(&.{}, columns[0..]);
    try inactive.prepareUpsert("users", "doc:inactive", inactive_row, null);
    try std.testing.expectEqual(@as(usize, 0), countDedicatedUniqueWrites(writes.items));
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var active = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    active.configureUniqueConstraints(&unique);
    active.configurePeriods(&.{}, columns[0..]);
    try active.prepareUpsert("users", "doc:active", active_row, null);
    try std.testing.expectEqual(@as(usize, 0), countDedicatedUniqueWrites(writes.items));
    try store.putBatch(writes.items, deletes.items);

    const logical_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, active_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(logical_tuple);
    const dedicated_key = try internal_keys.relationalUniqueKeyAlloc(alloc, unique[0].name, logical_tuple);
    defer alloc.free(dedicated_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, dedicated_key));

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var committed_duplicate = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    committed_duplicate.configureUniqueConstraints(&unique);
    committed_duplicate.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.UniqueConstraintViolation, committed_duplicate.prepareUpsert("users", "doc:duplicate", active_row, null));

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var same_batch = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    same_batch.configureUniqueConstraints(&unique);
    same_batch.configurePeriods(&.{}, columns[0..]);
    try same_batch.prepareUpsert("users", "doc:batch:a", batch_row, null);
    try std.testing.expectError(error.UniqueConstraintViolation, same_batch.prepareUpsert("users", "doc:batch:b", batch_row, null));
}

test "relational ordered tuple unique maintenance respects lifecycle states" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{
        .{ .name = "users_ready_key", .columns = &.{ "tenant_id", "email" } },
        .{ .name = "users_building_key", .columns = &.{ "tenant_id", "email" }, .validation_state = .unvalidated },
        .{ .name = "users_catching_up_key", .columns = &.{ "tenant_id", "email" }, .validation_state = .unvalidated },
        .{ .name = "users_stale_key", .columns = &.{ "tenant_id", "email" }, .validation_state = .unvalidated },
        .{ .name = "users_failed_key", .columns = &.{ "tenant_id", "email" }, .validation_state = .unvalidated },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "users_ready_key",
            .owner_kind = .unique_constraint,
            .owner_name = "users_ready_key",
            .access_method = .ordered_tuple,
            .unique = true,
            .columns = &.{ "tenant_id", "email" },
            .keys = index_keys[0..],
            .lifecycle = .ready,
            .generation = 7,
            .schema_fingerprint = "unique-index-v1:users_ready_key",
            .generation_record = .{ .generation = 7, .lifecycle = .ready },
        },
        .{
            .name = "users_building_key",
            .owner_kind = .unique_constraint,
            .owner_name = "users_building_key",
            .access_method = .ordered_tuple,
            .unique = true,
            .columns = &.{ "tenant_id", "email" },
            .keys = index_keys[0..],
            .lifecycle = .building,
            .generation = 7,
            .schema_fingerprint = "unique-index-v1:users_building_key",
            .generation_record = .{ .generation = 7, .lifecycle = .building },
        },
        .{
            .name = "users_catching_up_key",
            .owner_kind = .unique_constraint,
            .owner_name = "users_catching_up_key",
            .access_method = .ordered_tuple,
            .unique = true,
            .columns = &.{ "tenant_id", "email" },
            .keys = index_keys[0..],
            .lifecycle = .catching_up,
            .generation = 7,
            .schema_fingerprint = "unique-index-v1:users_catching_up_key",
            .generation_record = .{ .generation = 7, .lifecycle = .catching_up },
        },
        .{
            .name = "users_stale_key",
            .owner_kind = .unique_constraint,
            .owner_name = "users_stale_key",
            .access_method = .ordered_tuple,
            .unique = true,
            .columns = &.{ "tenant_id", "email" },
            .keys = index_keys[0..],
            .lifecycle = .stale,
            .generation = 7,
            .schema_fingerprint = "unique-index-v1:users_stale_key",
            .generation_record = .{ .generation = 7, .lifecycle = .stale },
        },
        .{
            .name = "users_failed_key",
            .owner_kind = .unique_constraint,
            .owner_name = "users_failed_key",
            .access_method = .ordered_tuple,
            .unique = true,
            .columns = &.{ "tenant_id", "email" },
            .keys = index_keys[0..],
            .lifecycle = .failed,
            .generation = 7,
            .schema_fingerprint = "unique-index-v1:users_failed_key",
            .generation_record = .{ .generation = 7, .lifecycle = .failed },
        },
    };
    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
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

    const countOrderedWrites = struct {
        fn count(items: []const docstore_mod.KVPair) usize {
            var n: usize = 0;
            for (items) |item| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(item.key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(item.key)) n += 1;
            }
            return n;
        }
    }.count;
    const countOrderedDeletes = struct {
        fn count(items: []const []const u8) usize {
            var n: usize = 0;
            for (items) |key| {
                if (internal_keys.isRelationalOrderedTupleIndexKey(key) or
                    internal_keys.isRelationalOrderedTupleIndexByDocKey(key)) n += 1;
            }
            return n;
        }
    }.count;

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    const tuple = try orderedTupleValueForIndexKeysAlloc(alloc, row, index_keys[0..], columns[0..]);
    defer alloc.free(tuple);

    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configurePeriods(&.{}, columns[0..]);
    try participant.prepareUpsert("users", "doc:a", row, null);
    try std.testing.expectEqual(@as(usize, 6), countOrderedWrites(writes.items));
    try store.putBatch(writes.items, deletes.items);

    const ready_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_ready_key", tuple, "", "");
    defer freeDocKeys(alloc, ready_docs);
    try std.testing.expectEqual(@as(usize, 1), ready_docs.len);
    const building_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_building_key", tuple, "", "");
    defer freeDocKeys(alloc, building_docs);
    try std.testing.expectEqual(@as(usize, 1), building_docs.len);
    const catching_up_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_catching_up_key", tuple, "", "");
    defer freeDocKeys(alloc, catching_up_docs);
    try std.testing.expectEqual(@as(usize, 1), catching_up_docs.len);
    const stale_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_stale_key", tuple, "", "");
    defer freeDocKeys(alloc, stale_docs);
    try std.testing.expectEqual(@as(usize, 0), stale_docs.len);
    const failed_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "users_failed_key", tuple, "", "");
    defer freeDocKeys(alloc, failed_docs);
    try std.testing.expectEqual(@as(usize, 0), failed_docs.len);

    const enforced_building_unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_building_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var enforced_building = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    enforced_building.configureUniqueConstraints(&enforced_building_unique);
    enforced_building.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.RelationalIndexNotReady, enforced_building.prepareUpsert("users", "doc:blocked", row, null));
    enforced_building.abort(null);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_participant.configureUniqueConstraints(&unique);
    delete_participant.configurePeriods(&.{}, columns[0..]);
    try delete_participant.prepareDelete("users", "doc:a", null);
    try std.testing.expectEqual(@as(usize, 6), countOrderedDeletes(deletes.items));
    try store.putBatch(writes.items, deletes.items);
}

test "relational foreign key parent checks use ready ordered tuple unique metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "unique-index-v1:users_tenant_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(child_row);
    const missing_child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "missing@example.test" } },
    });
    defer alloc.free(missing_child_row);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var parent_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    parent_participant.configureUniqueConstraints(&unique);
    parent_participant.configurePeriods(&.{}, columns[0..]);
    try parent_participant.prepareUpsert("rows", "user:ada", parent_row, null);
    try parent_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const unique_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, parent_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(unique_tuple);
    const dedicated_unique_key = try internal_keys.relationalUniqueKeyAlloc(alloc, unique[0].name, unique_tuple);
    defer alloc.free(dedicated_unique_key);
    const dedicated_unique_deletes = [_][]const u8{dedicated_unique_key};
    try store.putBatch(&.{}, dedicated_unique_deletes[0..]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var child_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    child_participant.configureUniqueConstraints(&unique);
    child_participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    child_participant.configurePeriods(&.{}, columns[0..]);
    try child_participant.prepareUpsert("rows", "order:1", child_row, null);
    try child_participant.commit(null, 2);
    try store.putBatch(writes.items, deletes.items);

    const stored_child = (try getRawAlloc(alloc, &store, "order:1")) orelse return error.TestExpectedEqual;
    alloc.free(stored_child);

    const stale_metadata_unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const stale_metadata_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
    }};
    const stale_metadata_policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], stale_metadata_indexes[0..]);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var stale_metadata_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, stale_metadata_policy);
    stale_metadata_participant.configureUniqueConstraints(&stale_metadata_unique);
    stale_metadata_participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    stale_metadata_participant.configurePeriods(&.{}, columns[0..]);
    try stale_metadata_participant.prepareUpsert("rows", "order:stale-metadata", child_row, null);
    try std.testing.expectError(error.ForeignKeyViolation, stale_metadata_participant.commit(null, 3));
    stale_metadata_participant.abort(null);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var missing_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    missing_participant.configureUniqueConstraints(&unique);
    missing_participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    missing_participant.configurePeriods(&.{}, columns[0..]);
    try missing_participant.prepareUpsert("rows", "order:missing", missing_child_row, null);
    try std.testing.expectError(error.ForeignKeyViolation, missing_participant.commit(null, 4));
    missing_participant.abort(null);

    const ordered_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, parent_row, index_keys[0..], columns[0..]);
    defer alloc.free(ordered_tuple);
    const ordered_forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, unique[0].name, ordered_tuple, "user:ada");
    defer alloc.free(ordered_forward_key);
    const ordered_reverse_key = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "user:ada", unique[0].name, ordered_tuple);
    defer alloc.free(ordered_reverse_key);
    const ordered_deletes = [_][]const u8{ ordered_forward_key, ordered_reverse_key };
    try store.putBatch(&.{}, ordered_deletes[0..]);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var missing_ordered_participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    missing_ordered_participant.configureUniqueConstraints(&unique);
    missing_ordered_participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    missing_ordered_participant.configurePeriods(&.{}, columns[0..]);
    try missing_ordered_participant.prepareUpsert("rows", "order:2", child_row, null);
    try std.testing.expectError(error.ForeignKeyViolation, missing_ordered_participant.commit(null, 5));
    missing_ordered_participant.abort(null);
}

test "relational foreign key parent checks use catalog-only ordered tuple unique metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "unique-index-v1:users_tenant_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:2" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "grace@example.test" } },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:2" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "grace@example.test" } },
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

    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    participant.configurePeriods(&.{}, columns[0..]);
    try participant.prepareUpsert("rows", "user:grace", parent_row, null);
    try participant.prepareUpsert("rows", "order:grace", child_row, null);

    const unique_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, parent_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(unique_tuple);
    const dedicated_unique_key = try internal_keys.relationalUniqueKeyAlloc(alloc, unique[0].name, unique_tuple);
    defer alloc.free(dedicated_unique_key);
    var write_index: usize = 0;
    while (write_index < writes.items.len) {
        if (std.mem.eql(u8, writes.items[write_index].key, dedicated_unique_key)) {
            _ = writes.orderedRemove(write_index);
            continue;
        }
        write_index += 1;
    }

    try participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const stored_parent = (try getRawAlloc(alloc, &store, "user:grace")) orelse return error.TestExpectedEqual;
    alloc.free(stored_parent);
    const stored_child = (try getRawAlloc(alloc, &store, "order:grace")) orelse return error.TestExpectedEqual;
    alloc.free(stored_child);
}

test "relational foreign key parent checks see staged ordered tuple unique parents" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "tenant_id" },
        .{ .column = "email" },
    };
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "users_tenant_email_key",
        .owner_kind = .unique_constraint,
        .owner_name = "users_tenant_email_key",
        .access_method = .ordered_tuple,
        .unique = true,
        .columns = &.{ "tenant_id", "email" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "unique-index-v1:users_tenant_email_key",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:2" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "grace@example.test" } },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:2" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "grace@example.test" } },
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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    participant.configureUniqueConstraints(&unique);
    participant.configureForeignKeys("rows", foreign_keys[0..], &.{});
    participant.configurePeriods(&.{}, columns[0..]);
    try participant.prepareUpsert("rows", "user:grace", parent_row, null);
    try participant.prepareUpsert("rows", "order:grace", child_row, null);

    const unique_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, parent_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(unique_tuple);
    const dedicated_unique_key = try internal_keys.relationalUniqueKeyAlloc(alloc, unique[0].name, unique_tuple);
    defer alloc.free(dedicated_unique_key);
    var write_index: usize = 0;
    while (write_index < writes.items.len) {
        if (std.mem.eql(u8, writes.items[write_index].key, dedicated_unique_key)) {
            _ = writes.orderedRemove(write_index);
            continue;
        }
        write_index += 1;
    }
    try participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const stored_parent = (try getRawAlloc(alloc, &store, "user:grace")) orelse return error.TestExpectedEqual;
    alloc.free(stored_parent);
    const stored_child = (try getRawAlloc(alloc, &store, "order:grace")) orelse return error.TestExpectedEqual;
    alloc.free(stored_child);
}

test "relational restrict parent delete uses child ordered tuple index" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const child_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "order_tenant_id" },
        .{ .column = "order_email" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_user_lookup_idx",
        .owner_kind = .relational_column,
        .owner_name = "order_tenant_id",
        .access_method = .ordered_tuple,
        .columns = &.{ "order_tenant_id", "order_email" },
        .keys = child_index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_user_lookup_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
        .on_delete = .restrict,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var seed = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    seed.configureUniqueConstraints(&unique);
    seed.configureForeignKeys("rows", foreign_keys[0..], &.{});
    seed.configurePeriods(&.{}, columns[0..]);
    try seed.prepareUpsert("rows", "user:ada", parent_row, null);
    try seed.prepareUpsert("rows", "order:ada", child_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const parent_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, parent_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(parent_tuple);
    const ref_prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, foreign_keys[0].name, foreign_keys[0].parent_table, parent_tuple);
    defer alloc.free(ref_prefix);
    const ref_upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, foreign_keys[0].name, foreign_keys[0].parent_table, parent_tuple);
    defer if (ref_upper) |buf| alloc.free(buf);
    const refs = try store.scanRange(alloc, ref_prefix, if (ref_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 1), refs.len);
    var ref_deletes = try alloc.alloc([]const u8, refs.len);
    defer alloc.free(ref_deletes);
    for (refs, 0..) |entry, index| ref_deletes[index] = entry.key;
    try store.putBatch(&.{}, ref_deletes);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_parent = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_parent.configureUniqueConstraints(&unique);
    delete_parent.configureForeignKeys("rows", foreign_keys[0..], &.{"user:ada"});
    delete_parent.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.ForeignKeyViolation, delete_parent.prepareDelete("rows", "user:ada", null));
    delete_parent.abort(null);
}

test "relational restrict parent delete proves child ordered tuple partial predicate from parent key" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const child_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "order_tenant_id" },
        .{ .column = "order_email" },
    };
    const child_index_where = [_]schema_mod.UniquePredicate{.{
        .field = "order_tenant_id",
        .op = .is_not_null,
    }};
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_user_lookup_idx",
        .owner_kind = .relational_column,
        .owner_name = "order_tenant_id",
        .access_method = .ordered_tuple,
        .columns = &.{ "order_tenant_id", "order_email" },
        .keys = child_index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_user_lookup_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
        .where = child_index_where[0..],
    }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
        .on_delete = .restrict,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "order_tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "order_email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var seed = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    seed.configureUniqueConstraints(&unique);
    seed.configureForeignKeys("rows", foreign_keys[0..], &.{});
    seed.configurePeriods(&.{}, columns[0..]);
    try seed.prepareUpsert("rows", "user:ada", parent_row, null);
    try seed.prepareUpsert("rows", "order:ada", child_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const parent_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, parent_row, unique[0], columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(parent_tuple);
    const ref_prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, foreign_keys[0].name, foreign_keys[0].parent_table, parent_tuple);
    defer alloc.free(ref_prefix);
    const ref_upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, foreign_keys[0].name, foreign_keys[0].parent_table, parent_tuple);
    defer if (ref_upper) |buf| alloc.free(buf);
    const refs = try store.scanRange(alloc, ref_prefix, if (ref_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, refs);
    var ref_deletes = try alloc.alloc([]const u8, refs.len);
    defer alloc.free(ref_deletes);
    for (refs, 0..) |entry, index| ref_deletes[index] = entry.key;
    try store.putBatch(&.{}, ref_deletes);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_parent = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_parent.configureUniqueConstraints(&unique);
    delete_parent.configureForeignKeys("rows", foreign_keys[0..], &.{"user:ada"});
    delete_parent.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.ForeignKeyViolation, delete_parent.prepareDelete("rows", "user:ada", null));
    delete_parent.abort(null);
}

test "relational restrict parent delete fails closed on unproved child ordered tuple predicate" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const child_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "order_tenant_id" },
        .{ .column = "order_email" },
    };
    const child_index_where = [_]schema_mod.UniquePredicate{.{
        .field = "status",
        .op = .eq,
        .value_json = "\"open\"",
    }};
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_user_lookup_idx",
        .owner_kind = .relational_column,
        .owner_name = "order_tenant_id",
        .access_method = .ordered_tuple,
        .columns = &.{ "order_tenant_id", "order_email" },
        .keys = child_index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_user_lookup_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
        .where = child_index_where[0..],
    }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
        .on_delete = .restrict,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(parent_row);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var seed = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    seed.configureUniqueConstraints(&unique);
    seed.configurePeriods(&.{}, columns[0..]);
    try seed.prepareUpsert("rows", "user:ada", parent_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_parent = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_parent.configureUniqueConstraints(&unique);
    delete_parent.configureForeignKeys("rows", foreign_keys[0..], &.{"user:ada"});
    delete_parent.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.ForeignKeyViolation, delete_parent.prepareDelete("rows", "user:ada", null));
    delete_parent.abort(null);
}

test "relational restrict parent delete fails closed on stale child ordered tuple index" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const child_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "order_tenant_id" },
        .{ .column = "order_email" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword, .indexed = false },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = false },
        .{ .name = "order_tenant_id", .path = "order_tenant_id", .field_type = .keyword },
        .{ .name = "order_email", .path = "order_email", .field_type = .keyword, .indexed = false },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_user_lookup_idx",
        .owner_kind = .relational_column,
        .owner_name = "order_tenant_id",
        .access_method = .ordered_tuple,
        .columns = &.{ "order_tenant_id", "order_email" },
        .keys = child_index_keys[0..],
        .lifecycle = .stale,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_user_lookup_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .stale },
    }};
    const unique = [_]schema_mod.UniqueConstraint{.{
        .name = "users_tenant_email_key",
        .columns = &.{ "tenant_id", "email" },
    }};
    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_user_email_fkey",
        .child_columns = &.{ "order_tenant_id", "order_email" },
        .parent_table = "rows",
        .parent_columns = &.{ "tenant_id", "email" },
        .on_delete = .restrict,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "tenant:1" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(parent_row);

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

    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    var seed = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    seed.configureUniqueConstraints(&unique);
    seed.configurePeriods(&.{}, columns[0..]);
    try seed.prepareUpsert("rows", "user:ada", parent_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_parent = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    delete_parent.configureUniqueConstraints(&unique);
    delete_parent.configureForeignKeys("rows", foreign_keys[0..], &.{"user:ada"});
    delete_parent.configurePeriods(&.{}, columns[0..]);
    try std.testing.expectError(error.ForeignKeyViolation, delete_parent.prepareDelete("rows", "user:ada", null));
    delete_parent.abort(null);
}

test "relational constraint tuple helpers reuse decoded row cells" {
    const alloc = std.testing.allocator;
    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada@Example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "ACTIVE" } },
        .{ .path = "code", .value_type = .bytes_val, .value = .{ .bytes_val = "MiXeD" } },
    });
    defer alloc.free(row);
    var decoded = try relational_row_codec.deserialize(alloc, row);
    defer decoded.deinit(alloc);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "email", .path = "email", .field_type = .keyword, .collation = "antfly.case_insensitive" },
        .{ .name = "status", .path = "status", .field_type = .keyword, .collation = "antfly.case_insensitive" },
        .{ .name = "code", .path = "code", .field_type = .keyword },
    };
    const expressions = [_]schema_mod.UniqueExpression{.{ .op = .lower, .field = "code" }};
    const constraint = schema_mod.UniqueConstraint{
        .name = "users_active_email_code_key",
        .columns = &.{"email"},
        .expressions = expressions[0..],
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    };
    const tuple_from_row = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, constraint, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple_from_row);
    const tuple_from_cells = (try uniqueConstraintTupleValueWithCellsAlloc(alloc, row, decoded.cells, constraint, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple_from_cells);
    try std.testing.expectEqualSlices(u8, tuple_from_row, tuple_from_cells);

    const primary_key = schema_mod.PrimaryKey{ .columns = &.{ "email", "code" } };
    const primary_from_row = try primaryKeyTupleValueAlloc(alloc, row, primary_key);
    defer alloc.free(primary_from_row);
    const primary_from_cells = try primaryKeyTupleValueFromCellsAlloc(alloc, decoded.cells, primary_key);
    defer alloc.free(primary_from_cells);
    try std.testing.expectEqualSlices(u8, primary_from_row, primary_from_cells);
}

test "relational partial unique predicates honor case-insensitive column collation" {
    const alloc = std.testing.allocator;

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "ACTIVE" } },
    });
    defer alloc.free(row);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "email", .path = "email", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword, .collation = "antfly.case_insensitive" },
    };
    const matching = schema_mod.UniqueConstraint{
        .name = "users_active_email_key",
        .columns = &.{"email"},
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    };
    const tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, matching, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple);

    const not_matching = schema_mod.UniqueConstraint{
        .name = "users_non_active_email_key",
        .columns = &.{"email"},
        .where = &.{.{ .field = "status", .op = .ne, .value_json = "\"active\"" }},
    };
    try std.testing.expect((try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, not_matching, columns[0..])) == null);

    const predicates = [_]schema_mod.RelationalCheck{.{ .name = "", .field = "status", .op = .eq, .value_json = "\"ACTIVE\"" }};
    try std.testing.expect(try predicatesImplyUniqueWhereWithColumns(alloc, predicates[0..], matching.where, columns[0..]));
    try std.testing.expect(!try predicatesImplyUniqueWhereWithColumns(alloc, predicates[0..], not_matching.where, columns[0..]));
}

test "relational partial unique is-not-null implication parses json null literals" {
    const alloc = std.testing.allocator;

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const where = [_]schema_mod.UniquePredicate{.{
        .field = "status",
        .op = .is_not_null,
    }};
    const pretty_null = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = " null ",
    }};
    const string_null = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = "\"null\"",
    }};
    const number_value = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = "42",
    }};
    const invalid_json = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = "not json",
    }};

    try std.testing.expect(!try predicatesImplyUniqueWhereWithColumns(alloc, pretty_null[0..], where[0..], columns[0..]));
    try std.testing.expect(try predicatesImplyUniqueWhereWithColumns(alloc, string_null[0..], where[0..], columns[0..]));
    try std.testing.expect(try predicatesImplyUniqueWhereWithColumns(alloc, number_value[0..], where[0..], columns[0..]));
    try std.testing.expect(!try predicatesImplyUniqueWhereWithColumns(alloc, invalid_json[0..], where[0..], columns[0..]));
}

test "relational expression partial unique predicates honor case-insensitive column collation" {
    const alloc = std.testing.allocator;

    const row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "ACTIVE" } },
    });
    defer alloc.free(row);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "email", .path = "email", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword, .collation = "antfly.case_insensitive" },
    };
    const rhs = [_]schema_mod.RelationalRowsExpression{.{ .kind = .value, .value_json = "\"active\"" }};
    const matching = schema_mod.UniqueConstraint{
        .name = "users_active_email_key",
        .columns = &.{"email"},
        .where_expressions = &.{.{
            .lhs = .{ .kind = .field, .field = "status" },
            .op = .eq,
            .rhs = rhs[0..],
        }},
    };
    const tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, matching, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(tuple);

    const exact_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "email", .path = "email", .field_type = .keyword },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    try std.testing.expect((try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, matching, exact_columns[0..])) == null);
}

test "relational foreign key unique reference values honor parent column collation" {
    const alloc = std.testing.allocator;

    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "ref_email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "Ada@Example.test" },
        },
    });
    defer alloc.free(child_row);
    const matching_child_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "ref_email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "ada@example.test" },
        },
    });
    defer alloc.free(matching_child_row);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "email", .path = "email", .field_type = .keyword, .collation = "antfly.case_insensitive" },
        .{ .name = "ref_email", .path = "ref_email", .field_type = .keyword },
    };
    const foreign_key = schema_mod.ForeignKey{
        .name = "users_ref_email_fkey",
        .child_columns = &.{"ref_email"},
        .parent_table = "row",
        .parent_columns = &.{"email"},
    };

    const parent_key = (try foreignKeyReferenceValueWithColumnsAlloc(alloc, child_row, foreign_key, null, &columns)) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_key);
    const matching_parent_key = (try foreignKeyReferenceValueWithColumnsAlloc(alloc, matching_child_row, foreign_key, null, &columns)) orelse return error.TestExpectedEqual;
    defer alloc.free(matching_parent_key);
    try std.testing.expectEqualSlices(u8, parent_key, matching_parent_key);

    const raw_parent_key = (try foreignKeyReferenceValueAlloc(alloc, child_row, foreign_key)) orelse return error.TestExpectedEqual;
    defer alloc.free(raw_parent_key);
    try std.testing.expect(!std.mem.eql(u8, parent_key, raw_parent_key));
}

test "relational foreign key reference helpers reuse decoded row cells" {
    const alloc = std.testing.allocator;

    const child_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "tenant_id", .value_type = .bytes_val, .value = .{ .bytes_val = "t1" } },
        .{ .path = "ref_email", .value_type = .bytes_val, .value = .{ .bytes_val = "Ada@Example.test" } },
        .{ .path = "customer_id", .value_type = .bytes_val, .value = .{ .bytes_val = "customer:1" } },
    });
    defer alloc.free(child_row);
    var decoded = try relational_row_codec.deserialize(alloc, child_row);
    defer decoded.deinit(alloc);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "tenant_id", .path = "tenant_id", .field_type = .keyword },
        .{ .name = "email", .path = "email", .field_type = .keyword, .collation = "antfly.case_insensitive" },
        .{ .name = "ref_email", .path = "ref_email", .field_type = .keyword },
        .{ .name = "customer_id", .path = "customer_id", .field_type = .keyword },
    };
    const composite_fk = schema_mod.ForeignKey{
        .name = "orders_customer_email_fkey",
        .child_columns = &.{ "tenant_id", "ref_email" },
        .parent_table = "row",
        .parent_columns = &.{ "tenant_id", "email" },
    };
    const primary_key = schema_mod.PrimaryKey{ .columns = &.{ "tenant_id", "email" } };
    const composite_from_row = (try foreignKeyReferenceValueWithColumnsAlloc(alloc, child_row, composite_fk, primary_key, columns[0..])) orelse return error.TestExpectedEqual;
    defer alloc.free(composite_from_row);
    const composite_from_cells = (try foreignKeyReferenceValueWithColumnsAndPrimaryKeyFromCellsAlloc(alloc, decoded.cells, composite_fk, primary_key, columns[0..])) orelse return error.TestExpectedEqual;
    defer alloc.free(composite_from_cells);
    try std.testing.expectEqualSlices(u8, composite_from_row, composite_from_cells);

    const primary_fk = schema_mod.ForeignKey{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "row",
        .parent_columns = &.{"_id"},
    };
    const primary_from_row = (try foreignKeyReferenceValueWithColumnsAlloc(alloc, child_row, primary_fk, null, columns[0..])) orelse return error.TestExpectedEqual;
    defer alloc.free(primary_from_row);
    const primary_from_cells = (try foreignKeyReferenceValueWithColumnsAndPrimaryKeyFromCellsAlloc(alloc, decoded.cells, primary_fk, null, columns[0..])) orelse return error.TestExpectedEqual;
    defer alloc.free(primary_from_cells);
    try std.testing.expectEqualSlices(u8, primary_from_row, primary_from_cells);
}

test "relational unique constraints encode ast expression tuple components" {
    const alloc = std.testing.allocator;

    const row_old = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "very old" },
        },
    });
    defer alloc.free(row_old);
    const row_new = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "very new" },
        },
    });
    defer alloc.free(row_new);
    const row_other = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "very current" },
        },
    });
    defer alloc.free(row_other);

    const operands = [_]schema_mod.RelationalRowsExpression{
        .{ .kind = .field, .field = "status" },
        .{ .kind = .value, .value_json = "\"old\"" },
        .{ .kind = .value, .value_json = "\"new\"" },
    };
    const constraint = schema_mod.UniqueConstraint{
        .name = "events_status_replace_key",
        .expressions = &.{.{
            .op = .expression,
            .expression = .{
                .kind = .replace,
                .operands = &operands,
            },
        }},
    };

    const old_tuple = (try uniqueConstraintTupleValueAlloc(alloc, row_old, constraint)) orelse return error.TestUnexpectedResult;
    defer alloc.free(old_tuple);
    const new_tuple = (try uniqueConstraintTupleValueAlloc(alloc, row_new, constraint)) orelse return error.TestUnexpectedResult;
    defer alloc.free(new_tuple);
    const other_tuple = (try uniqueConstraintTupleValueAlloc(alloc, row_other, constraint)) orelse return error.TestUnexpectedResult;
    defer alloc.free(other_tuple);

    try std.testing.expectEqualSlices(u8, old_tuple, new_tuple);
    try std.testing.expect(!std.mem.eql(u8, old_tuple, other_tuple));
}

test "relational unique ast field expressions honor source column collation and null distinctness" {
    const alloc = std.testing.allocator;

    const row_upper = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "Ada@Example.test" },
        },
    });
    defer alloc.free(row_upper);
    const row_lower = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "ada@example.test" },
        },
    });
    defer alloc.free(row_lower);
    const row_null = try relational_row_codec.serialize(alloc, &.{});
    defer alloc.free(row_null);

    const columns = [_]schema_mod.RelationalColumn{.{
        .name = "email",
        .path = "email",
        .field_type = .keyword,
        .collation = "antfly.case_insensitive",
    }};
    const expression = schema_mod.RelationalRowsExpression{
        .kind = .field,
        .field = "email",
    };
    const default_unique = schema_mod.UniqueConstraint{
        .name = "users_email_expr_key",
        .expressions = &.{.{ .op = .expression, .expression = expression }},
    };

    const upper_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_upper, default_unique, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(upper_tuple);
    const lower_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_lower, default_unique, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(lower_tuple);
    try std.testing.expectEqualSlices(u8, upper_tuple, lower_tuple);
    try std.testing.expect((try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_null, default_unique, columns[0..])) == null);

    const nulls_not_distinct_unique = schema_mod.UniqueConstraint{
        .name = "users_email_expr_key",
        .expressions = &.{.{ .op = .expression, .expression = expression }},
        .nulls_not_distinct = true,
    };
    const null_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_null, nulls_not_distinct_unique, columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(null_tuple);
    var expected_null_tuple = std.ArrayListUnmanaged(u8).empty;
    defer expected_null_tuple.deinit(alloc);
    try internal_keys.appendEncodedComponent(&expected_null_tuple, alloc, typedJsonNullValue());
    try std.testing.expectEqualSlices(u8, expected_null_tuple.items, null_tuple);

    const exact_columns = [_]schema_mod.RelationalColumn{.{
        .name = "email",
        .path = "email",
        .field_type = .keyword,
    }};
    const exact_upper_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_upper, default_unique, exact_columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(exact_upper_tuple);
    const exact_lower_tuple = (try uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_lower, default_unique, exact_columns[0..])) orelse return error.TestUnexpectedResult;
    defer alloc.free(exact_lower_tuple);
    try std.testing.expect(!std.mem.eql(u8, exact_upper_tuple, exact_lower_tuple));
}

test "relational bytes tuple helper matches row unique tuple encoding" {
    const alloc = std.testing.allocator;

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "ada@example.test" },
        },
        .{
            .path = "tenant",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "tenant:1" },
        },
    });
    defer alloc.free(row);
    const constraint = schema_mod.UniqueConstraint{
        .name = "users_email_tenant_key",
        .columns = &.{ "email", "tenant" },
    };

    const from_row = (try uniqueConstraintTupleValueAlloc(alloc, row, constraint)) orelse return error.TestUnexpectedResult;
    defer alloc.free(from_row);
    const from_values = try bytesTupleValueAlloc(alloc, &.{ "ada@example.test", "tenant:1" });
    defer alloc.free(from_values);

    try std.testing.expectEqualSlices(u8, from_row, from_values);
}

test "relational foreign key reference extraction implements match simple for composite nullable components" {
    const alloc = std.testing.allocator;
    const foreign_key = schema_mod.ForeignKey{
        .name = "orders_customer_email_fkey",
        .child_columns = &.{ "tenant_id", "customer_email" },
        .parent_table = "row",
        .parent_columns = &.{ "tenant_id", "email" },
        .match = .simple,
    };

    const complete_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "tenant_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "tenant:1" },
        },
        .{
            .path = "customer_email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "ada@example.test" },
        },
    });
    defer alloc.free(complete_row);
    const complete_parent = (try foreignKeyReferenceValueAlloc(alloc, complete_row, foreign_key)) orelse return error.TestExpectedEqual;
    defer alloc.free(complete_parent);
    try std.testing.expect(complete_parent.len > 0);

    const missing_component_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "tenant_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "tenant:1" },
        },
    });
    defer alloc.free(missing_component_row);
    try std.testing.expect((try foreignKeyReferenceValueAlloc(alloc, missing_component_row, foreign_key)) == null);
}

test "relational foreign key reference extraction implements match full for composite nullable components" {
    const alloc = std.testing.allocator;
    const foreign_key = schema_mod.ForeignKey{
        .name = "orders_customer_email_fkey",
        .child_columns = &.{ "tenant_id", "customer_email" },
        .parent_table = "row",
        .parent_columns = &.{ "tenant_id", "email" },
        .match = .full,
    };

    const complete_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "tenant_id",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "tenant:1" },
        },
        .{
            .path = "customer_email",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "ada@example.test" },
        },
    });
    defer alloc.free(complete_row);
    const complete_parent = (try foreignKeyReferenceValueAlloc(alloc, complete_row, foreign_key)) orelse return error.TestExpectedEqual;
    defer alloc.free(complete_parent);
    try std.testing.expect(complete_parent.len > 0);

    const all_null_row = try relational_row_codec.serialize(alloc, &.{});
    defer alloc.free(all_null_row);
    try std.testing.expect((try foreignKeyReferenceValueAlloc(alloc, all_null_row, foreign_key)) == null);

    const partial_null_row = try relational_row_codec.serialize(alloc, &.{.{
        .path = "tenant_id",
        .value_type = .bytes_val,
        .value = .{ .bytes_val = "tenant:1" },
    }});
    defer alloc.free(partial_null_row);
    try std.testing.expectError(error.ForeignKeyViolation, foreignKeyReferenceValueAlloc(alloc, partial_null_row, foreign_key));
}

test "relational write participant rejects partial match full composite foreign key references" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

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

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_email_fkey",
        .child_columns = &.{ "tenant_id", "customer_email" },
        .parent_table = "row",
        .parent_columns = &.{ "tenant_id", "email" },
        .match = .full,
    }};
    var participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    participant.configureForeignKeys("row", foreign_keys[0..], &.{});

    const partial_null_row = try relational_row_codec.serialize(alloc, &.{.{
        .path = "tenant_id",
        .value_type = .bytes_val,
        .value = .{ .bytes_val = "tenant:1" },
    }});
    defer alloc.free(partial_null_row);

    try std.testing.expectError(error.ForeignKeyViolation, participant.prepareUpsert("row", "order:partial", partial_null_row, null));
    participant.abort(null);
}

test "relational write participant defers no action parent delete until final state" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "row",
        .parent_columns = &.{"_id"},
        .on_delete = .no_action,
        .timing = .deferred,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{});
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{.{
        .path = "customer_id",
        .value_type = .bytes_val,
        .value = .{ .bytes_val = "customer:deferred-delete" },
    }});
    defer alloc.free(child_row);
    const child_without_ref = try relational_row_codec.serialize(alloc, &.{});
    defer alloc.free(child_without_ref);

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

    var seed = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    seed.configureForeignKeys("row", foreign_keys[0..], &.{});
    try seed.prepareUpsert("row", "customer:deferred-delete", parent_row, null);
    try seed.prepareUpsert("row", "order:deferred-delete", child_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();

    var deferred_delete = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    deferred_delete.configureForeignKeys("row", foreign_keys[0..], &.{"customer:deferred-delete"});
    try deferred_delete.prepareDelete("row", "customer:deferred-delete", null);
    try deferred_delete.prepareUpsert("row", "order:deferred-delete", child_without_ref, null);
    try deferred_delete.commit(null, 2);
    try store.putBatch(writes.items, deletes.items);

    try std.testing.expect((try getRawAlloc(alloc, &store, "customer:deferred-delete")) == null);
    const child_after = (try getMaterializedAlloc(alloc, &store, "order:deferred-delete")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_after);
    try std.testing.expectEqualStrings("{}", child_after);
}

test "relational write participant ignores unvalidated foreign keys for write enforcement" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "row",
        .parent_columns = &.{"_id"},
        .validation_state = .unvalidated,
    }};

    const child_row = try relational_row_codec.serialize(alloc, &.{.{
        .path = "customer_id",
        .value_type = .bytes_val,
        .value = .{ .bytes_val = "customer:missing" },
    }});
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

    var participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    participant.configureForeignKeys("row", foreign_keys[0..], &.{});
    try participant.prepareUpsert("row", "order:unvalidated", child_row, null);
    try participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    const stored_child = (try getRawAlloc(alloc, &store, "order:unvalidated")) orelse return error.TestExpectedEqual;
    alloc.free(stored_child);

    const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, "orders_customer_id_fkey", "row", "customer:missing");
    defer alloc.free(prefix);
    const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, "orders_customer_id_fkey", "row", "customer:missing");
    defer if (upper) |buf| alloc.free(buf);
    const refs = try store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 0), refs.len);
}

test "relational write participant can force deferred no action constraint immediate" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "row",
        .parent_columns = &.{"_id"},
        .on_delete = .no_action,
        .timing = .deferred,
    }};
    const timing_overrides = [_]ForeignKeyConstraintTimingOverride{.{
        .constraint_name = "orders_customer_id_fkey",
        .timing = .immediate,
    }};

    const parent_row = try relational_row_codec.serialize(alloc, &.{});
    defer alloc.free(parent_row);
    const child_row = try relational_row_codec.serialize(alloc, &.{.{
        .path = "customer_id",
        .value_type = .bytes_val,
        .value = .{ .bytes_val = "customer:forced-immediate" },
    }});
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

    var seed = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    seed.configureForeignKeys("row", foreign_keys[0..], &.{});
    try seed.prepareUpsert("row", "customer:forced-immediate", parent_row, null);
    try seed.prepareUpsert("row", "order:forced-immediate", child_row, null);
    try seed.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();

    var immediate_delete = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    immediate_delete.configureForeignKeys("row", foreign_keys[0..], &.{"customer:forced-immediate"});
    immediate_delete.configureForeignKeyConstraintTimingOverrides(timing_overrides[0..]);
    try std.testing.expectError(
        error.ForeignKeyViolation,
        immediate_delete.prepareDelete("row", "customer:forced-immediate", null),
    );
    immediate_delete.abort(null);

    const parent_after = (try getRawAlloc(alloc, &store, "customer:forced-immediate")) orelse return error.TestExpectedEqual;
    alloc.free(parent_after);
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
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{});
    try create_participant.prepareUpsert("row", "customer:wide", parent_row, null);
    try create_participant.prepareUpsert("row", "order:wide:1", child_one_row, null);
    try create_participant.prepareUpsert("row", "order:wide:2", child_two_row, null);
    try create_participant.commit(null, 1);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    var delete_participant = WriteParticipant.init(alloc, &store, &writes, &deletes, &owned_keys, &owned_values);
    delete_participant.configureForeignKeys("row", foreign_keys[0..], &.{"customer:wide"});
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
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{});
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
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{});
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
    create_participant.configureForeignKeys("row", foreign_keys[0..], &.{});
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
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_amount_index_key));

    const amounts_after_update = try scanColumnAlloc(alloc, &store, "amount", "doc:a", "doc:c");
    defer freeColumnValues(alloc, amounts_after_update);
    try std.testing.expectEqual(@as(usize, 1), amounts_after_update.len);
    try std.testing.expectEqualStrings("doc:a", amounts_after_update[0].doc_key);

    const doc_a_amount_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(doc_a_amount_index_key);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendDelete(alloc, &store, &deletes, &owned_keys, "doc:a");
    try store.putBatch(writes.items, deletes.items);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_a_amount_index_key));
}

test "relational upsert skips unchanged secondary index churn" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

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

    const initial_cells = [_]relational_row_codec.Cell{.{
        .path = "amount",
        .value_type = .f64_val,
        .value = .{ .f64_val = 10.0 },
    }};
    const initial_row = try relational_row_codec.serialize(alloc, initial_cells[0..]);
    defer alloc.free(initial_row);
    const columns = [_]schema_mod.RelationalColumn{.{
        .name = "amount",
        .path = "amount",
        .field_type = .numeric,
    }};
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "amount",
        .owner_kind = .relational_column,
        .owner_name = "amount",
        .access_method = .scalar_column,
        .columns = &.{"amount"},
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:amount",
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", initial_row, policy);
    try std.testing.expectEqual(@as(usize, 3), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    try store.putBatch(writes.items, deletes.items);

    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", initial_row, policy);
    try std.testing.expectEqual(@as(usize, 1), writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), deletes.items.len);
    try store.putBatch(writes.items, deletes.items);

    const changed_cells = [_]relational_row_codec.Cell{.{
        .path = "amount",
        .value_type = .f64_val,
        .value = .{ .f64_val = 20.0 },
    }};
    const changed_row = try relational_row_codec.serialize(alloc, changed_cells[0..]);
    defer alloc.free(changed_row);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", changed_row, policy);
    try std.testing.expectEqual(@as(usize, 3), writes.items.len);
    try std.testing.expectEqual(@as(usize, 2), deletes.items.len);
}

test "relational array element indexes support targeted containment scans" {
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
            .path = "tags",
            .value_type = .bytes_val,
            .is_json = true,
            .value = .{ .bytes_val = "[\"hot\",\"new\",\"hot\"]" },
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
            .path = "tags",
            .value_type = .bytes_val,
            .is_json = true,
            .value = .{ .bytes_val = "[\"cold\"]" },
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

    var parsed_hot = try std.json.parseFromSlice(std.json.Value, alloc, "\"hot\"", .{});
    defer parsed_hot.deinit();
    const hot_key = try arrayElementIndexKeyForValueAlloc(alloc, parsed_hot.value);
    defer alloc.free(hot_key);
    const hot_index_key = try internal_keys.relationalArrayElementIndexKeyAlloc(alloc, "tags", hot_key, "doc:a");
    defer alloc.free(hot_index_key);
    const hot_index_value = try store.get(alloc, hot_index_key);
    defer alloc.free(hot_index_value);

    const hot_docs = try scanArrayElementDocKeysAlloc(alloc, &store, "tags", hot_key, "doc:a", "doc:z");
    defer freeDocKeys(alloc, hot_docs);
    try std.testing.expectEqual(@as(usize, 1), hot_docs.len);
    try std.testing.expectEqualStrings("doc:a", hot_docs[0]);

    const row_a_cold = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "title",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "alpha" },
        },
        .{
            .path = "tags",
            .value_type = .bytes_val,
            .is_json = true,
            .value = .{ .bytes_val = "[\"cold\"]" },
        },
    });
    defer alloc.free(row_a_cold);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a_cold);
    try store.putBatch(writes.items, deletes.items);

    try std.testing.expectError(error.NotFound, store.get(alloc, hot_index_key));
    const hot_after_update = try scanArrayElementDocKeysAlloc(alloc, &store, "tags", hot_key, "doc:a", "doc:z");
    defer freeDocKeys(alloc, hot_after_update);
    try std.testing.expectEqual(@as(usize, 0), hot_after_update.len);

    var parsed_cold = try std.json.parseFromSlice(std.json.Value, alloc, "\"cold\"", .{});
    defer parsed_cold.deinit();
    const cold_key = try arrayElementIndexKeyForValueAlloc(alloc, parsed_cold.value);
    defer alloc.free(cold_key);
    const cold_docs = try scanArrayElementDocKeysAlloc(alloc, &store, "tags", cold_key, "doc:a", "doc:z");
    defer freeDocKeys(alloc, cold_docs);
    try std.testing.expectEqual(@as(usize, 2), cold_docs.len);
    try std.testing.expectEqualStrings("doc:a", cold_docs[0]);
    try std.testing.expectEqualStrings("doc:b", cold_docs[1]);
}

test "relational json value indexes support targeted containment scans" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{.{
        .path = "attrs",
        .value_type = .bytes_val,
        .is_json = true,
        .value = .{ .bytes_val = "{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\",\"beta\"]}" },
    }});
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{.{
        .path = "attrs",
        .value_type = .bytes_val,
        .is_json = true,
        .value = .{ .bytes_val = "{\"billing\":{\"plan\":\"free\"},\"flags\":[\"active\"]}" },
    }});
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

    var parsed_pro = try std.json.parseFromSlice(std.json.Value, alloc, "\"pro\"", .{});
    defer parsed_pro.deinit();
    const pro_key = try jsonValueIndexKeyForValueAlloc(alloc, parsed_pro.value);
    defer alloc.free(pro_key);
    const pro_index_key = try internal_keys.relationalJsonValueIndexKeyAlloc(alloc, "attrs", "billing.plan", pro_key, "doc:a");
    defer alloc.free(pro_index_key);
    const pro_index_value = try store.get(alloc, pro_index_key);
    defer alloc.free(pro_index_value);

    var wanted = try std.json.parseFromSlice(std.json.Value, alloc, "{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"active\"]}", .{});
    defer wanted.deinit();
    const pro_ids = try scanJsonContainmentDocKeysAlloc(alloc, &store, "attrs", wanted.value, "", "");
    defer freeDocKeys(alloc, pro_ids);
    try std.testing.expectEqual(@as(usize, 1), pro_ids.len);
    try std.testing.expectEqualStrings("doc:a", pro_ids[0]);

    const row_a_updated = try relational_row_codec.serialize(alloc, &.{.{
        .path = "attrs",
        .value_type = .bytes_val,
        .is_json = true,
        .value = .{ .bytes_val = "{\"billing\":{\"plan\":\"free\"},\"flags\":[\"active\"]}" },
    }});
    defer alloc.free(row_a_updated);
    writes.clearRetainingCapacity();
    deletes.clearRetainingCapacity();
    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a_updated);
    try store.putBatch(writes.items, deletes.items);

    try std.testing.expectError(error.NotFound, store.get(alloc, pro_index_key));
    const pro_ids_after = try scanJsonContainmentDocKeysAlloc(alloc, &store, "attrs", wanted.value, "", "");
    defer freeDocKeys(alloc, pro_ids_after);
    try std.testing.expectEqual(@as(usize, 0), pro_ids_after.len);
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

test "relational column indexing policy stores schema rows without per-column cells" {
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
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "title",
        .owner_kind = .relational_column,
        .owner_name = "title",
        .access_method = .scalar_column,
        .columns = &.{"title"},
        .lifecycle = .ready,
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    try appendUpsertWithColumnIndexPolicy(
        alloc,
        &store,
        &writes,
        &deletes,
        &owned_keys,
        &owned_values,
        "doc:a",
        row,
        policy,
    );
    try store.putBatch(writes.items, deletes.items);

    const row_key = try rowKeyAlloc(alloc, "doc:a");
    defer alloc.free(row_key);
    const stored_row = try store.get(alloc, row_key);
    defer alloc.free(stored_row);
    const stored_amount_cell = (try relational_row_codec.findCellByPath(stored_row, "amount")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(f64, 42.0), stored_amount_cell.value.f64_val);

    const amount_cell_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "amount");
    defer alloc.free(amount_cell_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, amount_cell_key));

    const title_cell_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:a", "title");
    defer alloc.free(title_cell_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, title_cell_key));

    const amount_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:a");
    defer alloc.free(amount_index_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, amount_index_key));

    const title_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "title", "doc:a");
    defer alloc.free(title_index_key);
    const title_index = try store.get(alloc, title_index_key);
    defer alloc.free(title_index);
}

test "relational partial column index policy writes only predicate-matching rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "ACTIVE" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "ada@example.test" } },
    });
    defer alloc.free(active_row);
    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "INACTIVE" } },
        .{ .path = "email", .value_type = .bytes_val, .value = .{ .bytes_val = "grace@example.test" } },
    });
    defer alloc.free(inactive_row);

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
        .{ .name = "status", .path = "status", .field_type = .keyword, .collation = "antfly.case_insensitive", .indexed = true },
        .{ .name = "email", .path = "email", .field_type = .keyword, .indexed = true, .index_lifecycle = .building, .index_where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }} },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "status",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .scalar_column,
            .columns = &.{"status"},
            .lifecycle = .ready,
        },
        .{
            .name = "email",
            .owner_kind = .relational_column,
            .owner_name = "email",
            .access_method = .scalar_column,
            .columns = &.{"email"},
            .lifecycle = .building,
            .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
        },
    };
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:active", active_row, policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:inactive", inactive_row, policy);
    try store.putBatch(writes.items, deletes.items);

    const active_email_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "doc:active");
    defer alloc.free(active_email_index_key);
    const active_email_index = try store.get(alloc, active_email_index_key);
    defer alloc.free(active_email_index);

    var participant = WriteParticipant.initWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, policy);
    defer participant.abort(null);
    try std.testing.expectError(error.RelationalIndexNotReady, participant.scanColumn("email", "doc:active", "doc:inactive", null));

    const ready_status_values = try participant.scanColumn("status", "doc:active", "doc:inactive", null);
    defer freeColumnValues(alloc, ready_status_values);
    try std.testing.expectEqual(@as(usize, 2), ready_status_values.len);

    const inactive_email_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "doc:inactive");
    defer alloc.free(inactive_email_index_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, inactive_email_index_key));

    const inactive_cell_key = try internal_keys.relationalColumnKeyAlloc(alloc, "doc:inactive", "email");
    defer alloc.free(inactive_cell_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, inactive_cell_key));
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

test "relational row rewrite renames and drops packed cells with fresh side indexes" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "open" },
        },
        .{
            .path = "count",
            .value_type = .f64_val,
            .value = .{ .f64_val = 3.0 },
        },
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 10.0 },
        },
        .{
            .path = "metadata",
            .value_type = .bytes_val,
            .is_json = true,
            .value = .{ .bytes_val = "{\"tier\":\"gold\"}" },
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

    try appendUpsert(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row);
    try store.putBatch(writes.items, deletes.items);

    const old_status_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "doc:a");
    defer alloc.free(old_status_index);
    const new_state_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "state", "doc:a");
    defer alloc.free(new_state_index);

    const old_status_index_value = try store.get(alloc, old_status_index);
    defer alloc.free(old_status_index_value);

    const report = try rewriteRowsInRange(
        alloc,
        &store,
        .{
            .renames = &.{.{ .old_path = "status", .new_path = "state" }},
            .drops = &.{"amount"},
            .sets = &.{.{
                .cell = .{
                    .path = "source",
                    .value_type = .bytes_val,
                    .value = .{ .bytes_val = "rewrite" },
                },
            }},
        },
        "doc:a",
        "doc:z",
    );
    try std.testing.expectEqual(@as(u64, 1), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), report.rewritten_rows);
    try std.testing.expectEqual(@as(u64, 1), report.renamed_cells);
    try std.testing.expectEqual(@as(u64, 1), report.dropped_cells);
    try std.testing.expectEqual(@as(u64, 1), report.set_cells);

    const materialized = (try getMaterializedAlloc(alloc, &store, "doc:a")).?;
    defer alloc.free(materialized);
    try std.testing.expectEqualStrings("{\"state\":\"open\",\"count\":3,\"metadata\":{\"tier\":\"gold\"},\"source\":\"rewrite\"}", materialized);

    try std.testing.expectError(error.NotFound, store.get(alloc, old_status_index));

    const new_state_index_value = try store.get(alloc, new_state_index);
    defer alloc.free(new_state_index_value);
}

test "relational row rewrite sets cells and honors column index policy" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "open" },
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
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true },
        .{ .name = "backfilled", .path = "backfilled", .field_type = .keyword, .indexed = false },
        .{ .name = "count", .path = "count", .field_type = .numeric, .indexed = true },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "status",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .scalar_column,
            .columns = &.{"status"},
            .lifecycle = .ready,
        },
        .{
            .name = "count",
            .owner_kind = .relational_column,
            .owner_name = "count",
            .access_method = .scalar_column,
            .columns = &.{"count"},
            .lifecycle = .ready,
        },
    };
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    try appendUpsertWithColumnIndexPolicy(
        alloc,
        &store,
        &writes,
        &deletes,
        &owned_keys,
        &owned_values,
        "doc:a",
        row,
        policy,
    );
    try store.putBatch(writes.items, deletes.items);

    const report = try rewriteRowsInRangeWithColumnIndexPolicy(
        alloc,
        &store,
        .{ .sets = &.{
            .{
                .cell = .{
                    .path = "status",
                    .value_type = .bytes_val,
                    .value = .{ .bytes_val = "closed" },
                },
            },
            .{
                .cell = .{
                    .path = "backfilled",
                    .value_type = .bytes_val,
                    .value = .{ .bytes_val = "defaulted" },
                },
            },
            .{
                .cell = .{
                    .path = "count",
                    .value_type = .f64_val,
                    .value = .{ .f64_val = 7.0 },
                },
                .only_if_missing = true,
            },
        } },
        "doc:a",
        "doc:z",
        policy,
    );
    try std.testing.expectEqual(@as(u64, 1), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), report.rewritten_rows);
    try std.testing.expectEqual(@as(u64, 3), report.set_cells);

    const raw_row_key = try internal_keys.relationalRowKeyAlloc(alloc, "doc:a");
    defer alloc.free(raw_row_key);
    const raw_row = try store.get(alloc, raw_row_key);
    defer alloc.free(raw_row);
    const status_cell = (try relational_row_codec.findCellByPath(raw_row, "status")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(typed_dv.ValueType.bytes_val, status_cell.value_type);
    try std.testing.expectEqualStrings("closed", status_cell.value.bytes_val);
    const count_cell = (try relational_row_codec.findCellByPath(raw_row, "count")) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(typed_dv.ValueType.f64_val, count_cell.value_type);
    try std.testing.expectEqual(@as(f64, 7.0), count_cell.value.f64_val);
    const materialized = (try getMaterializedAlloc(alloc, &store, "doc:a")).?;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"backfilled\":\"defaulted\"") != null);

    const backfilled_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "backfilled", "doc:a");
    defer alloc.free(backfilled_index);
    try std.testing.expectError(error.NotFound, store.get(alloc, backfilled_index));

    const count_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "count", "doc:a");
    defer alloc.free(count_index);
    const count_index_value = try store.get(alloc, count_index);
    defer alloc.free(count_index_value);
}

test "relational secondary index rebuild row pager streams bounded rows with resume key" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "open" },
        },
    });
    defer alloc.free(row);

    const row_a_key = try rowKeyAlloc(alloc, "doc:a");
    defer alloc.free(row_a_key);
    const row_b_key = try rowKeyAlloc(alloc, "doc:b");
    defer alloc.free(row_b_key);
    const row_c_key = try rowKeyAlloc(alloc, "doc:c");
    defer alloc.free(row_c_key);
    try store.putBatch(&.{
        .{ .key = row_a_key, .value = row },
        .{ .key = row_b_key, .value = row },
        .{ .key = row_c_key, .value = row },
        .{ .key = "nonrelational:ignored", .value = row },
    }, &.{});

    try std.testing.expectError(error.InvalidSecondaryIndexRebuildRequest, scanRowsSpanPageAlloc(alloc, &store, "doc:a", "doc:z", 0));

    var first = try scanRowsSpanPageAlloc(alloc, &store, "doc:a", "doc:z", 2);
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), first.rows.len);
    try std.testing.expectEqualStrings("doc:a", first.rows[0].doc_key);
    try std.testing.expectEqualStrings("doc:b", first.rows[1].doc_key);
    try std.testing.expectEqualStrings("doc:c", first.next_start_doc_key);

    var second = try scanRowsSpanPageAlloc(alloc, &store, first.next_start_doc_key, "doc:z", 2);
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second.rows.len);
    try std.testing.expectEqualStrings("doc:c", second.rows[0].doc_key);
    try std.testing.expectEqualStrings("", second.next_start_doc_key);
}

test "relational secondary index range rebuild repairs only target building index generation" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 1.0 },
        },
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "active" },
        },
    });
    defer alloc.free(active_row);
    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{
            .path = "amount",
            .value_type = .f64_val,
            .value = .{ .f64_val = 2.0 },
        },
        .{
            .path = "status",
            .value_type = .bytes_val,
            .value = .{ .bytes_val = "inactive" },
        },
    });
    defer alloc.free(inactive_row);

    const initial_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true },
        .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true },
    };
    const initial_indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "amount",
            .owner_kind = .relational_column,
            .owner_name = "amount",
            .access_method = .scalar_column,
            .columns = &.{"amount"},
            .lifecycle = .ready,
        },
        .{
            .name = "status",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .scalar_column,
            .columns = &.{"status"},
            .lifecycle = .ready,
        },
    };
    const initial_policy = ColumnIndexPolicy.fromSchemaParts(initial_columns[0..], initial_indexes[0..]);
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
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:active", active_row, initial_policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:inactive", inactive_row, initial_policy);
    try store.putBatch(writes.items, deletes.items);

    const active_amount_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:active");
    defer alloc.free(active_amount_index);
    const inactive_amount_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:inactive");
    defer alloc.free(inactive_amount_index);
    const inactive_status_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "status", "doc:inactive");
    defer alloc.free(inactive_status_index);

    const inactive_amount_before = try store.get(alloc, inactive_amount_index);
    defer alloc.free(inactive_amount_before);

    const rebuild_columns = [_]schema_mod.RelationalColumn{.{
        .name = "amount",
        .path = "amount",
        .field_type = .numeric,
    }};
    const rebuild_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "amount",
        .owner_kind = .relational_column,
        .owner_name = "amount",
        .access_method = .scalar_column,
        .columns = &.{"amount"},
        .lifecycle = .building,
        .generation = 7,
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    }};
    const rebuild_policy = ColumnIndexPolicy.fromSchemaParts(rebuild_columns[0..], rebuild_indexes[0..]);
    try std.testing.expectError(
        error.RelationalIndexGenerationMismatch,
        rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(alloc, &store, "amount", 8, "doc:active", "doc:z", rebuild_policy),
    );

    const report = try rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(alloc, &store, "amount", 7, "doc:active", "doc:z", rebuild_policy);
    try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), report.indexed_rows);
    try std.testing.expect(report.deleted_entries >= 4);
    try std.testing.expectEqual(@as(u64, 2), report.written_entries);

    const active_amount_after = try store.get(alloc, active_amount_index);
    defer alloc.free(active_amount_after);
    try std.testing.expectError(error.NotFound, store.get(alloc, inactive_amount_index));
    const inactive_status_after = try store.get(alloc, inactive_status_index);
    defer alloc.free(inactive_status_after);
}

test "relational secondary index range rebuild uses catalog-only scalar index metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
    });
    defer alloc.free(active_row);
    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
    });
    defer alloc.free(inactive_row);

    const row_active_key = try rowKeyAlloc(alloc, "doc:active");
    defer alloc.free(row_active_key);
    const row_inactive_key = try rowKeyAlloc(alloc, "doc:inactive");
    defer alloc.free(row_inactive_key);
    try store.putBatch(&.{
        .{ .key = row_active_key, .value = active_row },
        .{ .key = row_inactive_key, .value = inactive_row },
    }, &.{});

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "amount",
        .access_method = .scalar_column,
        .columns = &.{"amount"},
        .lifecycle = .building,
        .generation = 7,
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

    try std.testing.expectError(
        error.RelationalIndexGenerationMismatch,
        rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(alloc, &store, "orders_amount_idx", 8, "doc:active", "doc:z", policy),
    );

    const report = try rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(alloc, &store, "orders_amount_idx", 7, "doc:active", "doc:z", policy);
    try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), report.indexed_rows);
    try std.testing.expectEqual(@as(u64, 2), report.written_entries);

    const active_amount_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:active");
    defer alloc.free(active_amount_index);
    const inactive_amount_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:inactive");
    defer alloc.free(inactive_amount_index);
    const active_amount_after = try store.get(alloc, active_amount_index);
    defer alloc.free(active_amount_after);
    try std.testing.expectError(error.NotFound, store.get(alloc, inactive_amount_index));
}

test "relational secondary index range rebuild repairs ordered tuple pages from packed rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "keep" } },
    });
    defer alloc.free(active_row);
    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "drop" } },
    });
    defer alloc.free(inactive_row);

    const initial_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const initial_include = [_][]const u8{"note"};
    const initial_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const initial_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .include_columns = initial_include[0..],
        .keys = initial_index_keys[0..],
        .lifecycle = .ready,
        .generation = 6,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 6, .lifecycle = .ready },
    }};
    const initial_policy = ColumnIndexPolicy.fromSchemaParts(initial_columns[0..], initial_indexes[0..]);

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
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:active", active_row, initial_policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:inactive", inactive_row, initial_policy);
    try store.putBatch(writes.items, deletes.items);

    const rebuild_index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const rebuild_include = [_][]const u8{"note"};
    const rebuild_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const rebuild_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .include_columns = rebuild_include[0..],
        .keys = rebuild_index_keys[0..],
        .lifecycle = .building,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .building },
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    }};
    const rebuild_policy = ColumnIndexPolicy.fromSchemaParts(rebuild_columns[0..], rebuild_indexes[0..]);
    var first = try rebuildColumnIndexFromRowsInSpanPageWithColumnIndexPolicy(
        alloc,
        &store,
        "orders_status_amount_idx",
        7,
        "doc:active",
        "doc:z",
        rebuild_policy,
        1,
    );
    defer first.deinit(alloc);
    try std.testing.expect(!first.complete);
    try std.testing.expectEqualStrings("doc:inactive", first.next_start_doc_key);
    try std.testing.expectEqual(@as(u64, 1), first.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), first.report.indexed_rows);
    try std.testing.expectEqual(@as(u64, 2), first.report.deleted_entries);
    try std.testing.expectEqual(@as(u64, 2), first.report.written_entries);

    var second = try rebuildColumnIndexFromRowsInSpanPageWithColumnIndexPolicy(
        alloc,
        &store,
        "orders_status_amount_idx",
        7,
        first.next_start_doc_key,
        "doc:z",
        rebuild_policy,
        1,
    );
    defer second.deinit(alloc);
    try std.testing.expect(second.complete);
    try std.testing.expectEqual(@as(u64, 1), second.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 0), second.report.indexed_rows);
    try std.testing.expectEqual(@as(u64, 2), second.report.deleted_entries);
    try std.testing.expectEqual(@as(u64, 0), second.report.written_entries);

    const active_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, active_row, rebuild_indexes[0].keys, rebuild_columns[0..]);
    defer alloc.free(active_tuple);
    const inactive_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, inactive_row, rebuild_indexes[0].keys, rebuild_columns[0..]);
    defer alloc.free(inactive_tuple);
    const active_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", active_tuple, "", "");
    defer freeDocKeys(alloc, active_docs);
    try std.testing.expectEqual(@as(usize, 1), active_docs.len);
    try std.testing.expectEqualStrings("doc:active", active_docs[0]);
    const inactive_docs = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", inactive_tuple, "", "");
    defer freeDocKeys(alloc, inactive_docs);
    try std.testing.expectEqual(@as(usize, 0), inactive_docs.len);

    const active_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", active_tuple, "doc:active");
    defer alloc.free(active_forward);
    const active_payload = try store.get(alloc, active_forward);
    defer alloc.free(active_payload);
    var payload_row = try relational_row_codec.deserialize(alloc, active_payload);
    defer payload_row.deinit(alloc);
    const note = findCellInRow(payload_row.cells, "note") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("keep", note.value.bytes_val);
}

test "relational ordered tuple maintenance and rebuild use catalog-only index metadata" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const active_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "active" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "keep" } },
    });
    defer alloc.free(active_row);
    const inactive_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "inactive" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "drop" } },
    });
    defer alloc.free(inactive_row);

    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const include_columns = [_][]const u8{"note"};
    const ready_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .include_columns = include_columns[0..],
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 6,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 6, .lifecycle = .ready },
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    }};
    const ready_policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], ready_indexes[0..]);

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
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:active", active_row, ready_policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:inactive", inactive_row, ready_policy);
    try store.putBatch(writes.items, deletes.items);

    const active_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, active_row, index_keys[0..], columns[0..]);
    defer alloc.free(active_tuple);
    const inactive_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, inactive_row, index_keys[0..], columns[0..]);
    defer alloc.free(inactive_tuple);
    const active_docs_before = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", active_tuple, "", "");
    defer freeDocKeys(alloc, active_docs_before);
    try std.testing.expectEqual(@as(usize, 1), active_docs_before.len);
    try std.testing.expectEqualStrings("doc:active", active_docs_before[0]);
    const inactive_docs_before = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", inactive_tuple, "", "");
    defer freeDocKeys(alloc, inactive_docs_before);
    try std.testing.expectEqual(@as(usize, 0), inactive_docs_before.len);

    const building_indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .include_columns = include_columns[0..],
        .keys = index_keys[0..],
        .lifecycle = .building,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .building },
        .where = &.{.{ .field = "status", .op = .eq, .value_json = "\"active\"" }},
    }};
    const building_policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], building_indexes[0..]);
    const report = try rebuildColumnIndexFromRowsInSpanWithColumnIndexPolicy(alloc, &store, "orders_status_amount_idx", 7, "doc:active", "doc:z", building_policy);
    try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), report.indexed_rows);
    try std.testing.expect(report.deleted_entries >= 2);
    try std.testing.expectEqual(@as(u64, 2), report.written_entries);

    const active_docs_after = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", active_tuple, "", "");
    defer freeDocKeys(alloc, active_docs_after);
    try std.testing.expectEqual(@as(usize, 1), active_docs_after.len);
    try std.testing.expectEqualStrings("doc:active", active_docs_after[0]);
    const inactive_docs_after = try scanOrderedTupleDocKeysAlloc(alloc, &store, "orders_status_amount_idx", inactive_tuple, "", "");
    defer freeDocKeys(alloc, inactive_docs_after);
    try std.testing.expectEqual(@as(usize, 0), inactive_docs_after.len);

    const active_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", active_tuple, "doc:active");
    defer alloc.free(active_forward);
    const active_payload = try store.get(alloc, active_forward);
    defer alloc.free(active_payload);
    var payload_row = try relational_row_codec.deserialize(alloc, active_payload);
    defer payload_row.deinit(alloc);
    const note = findCellInRow(payload_row.cells, "note") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("keep", note.value.bytes_val);
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

test "relational column scan delete by document range removes ordered tuple entries" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
    });
    defer alloc.free(row_b);

    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
    };
    const indexes = [_]schema_mod.RelationalIndex{.{
        .name = "orders_status_amount_idx",
        .owner_kind = .relational_column,
        .owner_name = "status",
        .access_method = .ordered_tuple,
        .columns = &.{ "status", "amount" },
        .keys = index_keys[0..],
        .lifecycle = .ready,
        .generation = 7,
        .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
        .generation_record = .{ .generation = 7, .lifecycle = .ready },
    }};
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

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

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a, policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b, policy);
    try store.putBatch(writes.items, deletes.items);

    const tuple_a = try orderedTupleValueForIndexKeysAlloc(alloc, row_a, index_keys[0..], columns[0..]);
    defer alloc.free(tuple_a);
    const tuple_b = try orderedTupleValueForIndexKeysAlloc(alloc, row_b, index_keys[0..], columns[0..]);
    defer alloc.free(tuple_b);
    const doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_a, "doc:a");
    defer alloc.free(doc_a_forward);
    const doc_b_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_b, "doc:b");
    defer alloc.free(doc_b_forward);
    const doc_a_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:a", "orders_status_amount_idx", tuple_a);
    defer alloc.free(doc_a_reverse);
    const doc_b_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:b", "orders_status_amount_idx", tuple_b);
    defer alloc.free(doc_b_reverse);

    const before_b_reverse = try store.get(alloc, doc_b_reverse);
    defer alloc.free(before_b_reverse);

    try deleteColumnIndexesByDocRange(alloc, &store, "doc:b", "doc:c");

    const remaining_a_forward = try store.get(alloc, doc_a_forward);
    defer alloc.free(remaining_a_forward);
    const remaining_a_reverse = try store.get(alloc, doc_a_reverse);
    defer alloc.free(remaining_a_reverse);
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_forward));
    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_reverse));
}

test "relational column backed repair rebuilds ordered tuple entries from packed rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const row_a = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 1.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "a" } },
    });
    defer alloc.free(row_a);
    const row_b = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "open" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 2.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "b" } },
    });
    defer alloc.free(row_b);
    const stale_row = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "status", .value_type = .bytes_val, .value = .{ .bytes_val = "closed" } },
        .{ .path = "amount", .value_type = .f64_val, .value = .{ .f64_val = 99.0 } },
        .{ .path = "note", .value_type = .bytes_val, .value = .{ .bytes_val = "stale" } },
    });
    defer alloc.free(stale_row);

    const index_keys = [_]schema_mod.RelationalIndexKey{
        .{ .column = "status" },
        .{ .column = "amount" },
    };
    const include_columns = [_][]const u8{"note"};
    const columns = [_]schema_mod.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "note", .path = "note", .field_type = .keyword },
    };
    const indexes = [_]schema_mod.RelationalIndex{
        .{
            .name = "orders_status_amount_idx",
            .owner_kind = .relational_column,
            .owner_name = "status",
            .access_method = .ordered_tuple,
            .columns = &.{ "status", "amount" },
            .include_columns = include_columns[0..],
            .keys = index_keys[0..],
            .lifecycle = .ready,
            .generation = 7,
            .schema_fingerprint = "secondary-index-v1:orders_status_amount_idx",
            .generation_record = .{ .generation = 7, .lifecycle = .ready },
        },
        .{
            .name = "amount",
            .owner_kind = .relational_column,
            .owner_name = "amount",
            .access_method = .scalar_column,
            .columns = &.{"amount"},
            .lifecycle = .ready,
        },
    };
    const policy = ColumnIndexPolicy.fromSchemaParts(columns[0..], indexes[0..]);

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

    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:a", row_a, policy);
    try appendUpsertWithColumnIndexPolicy(alloc, &store, &writes, &deletes, &owned_keys, &owned_values, "doc:b", row_b, policy);
    try store.putBatch(writes.items, deletes.items);

    const tuple_a = try orderedTupleValueForIndexKeysAlloc(alloc, row_a, index_keys[0..], columns[0..]);
    defer alloc.free(tuple_a);
    const tuple_b = try orderedTupleValueForIndexKeysAlloc(alloc, row_b, index_keys[0..], columns[0..]);
    defer alloc.free(tuple_b);
    const stale_tuple = try orderedTupleValueForIndexKeysAlloc(alloc, stale_row, index_keys[0..], columns[0..]);
    defer alloc.free(stale_tuple);

    const doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_a, "doc:a");
    defer alloc.free(doc_a_forward);
    const doc_a_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:a", "orders_status_amount_idx", tuple_a);
    defer alloc.free(doc_a_reverse);
    const doc_b_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", tuple_b, "doc:b");
    defer alloc.free(doc_b_forward);
    const doc_b_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:b", "orders_status_amount_idx", tuple_b);
    defer alloc.free(doc_b_reverse);
    const stale_doc_a_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", stale_tuple, "doc:a");
    defer alloc.free(stale_doc_a_forward);
    const stale_doc_z_forward = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_amount_idx", stale_tuple, "doc:z");
    defer alloc.free(stale_doc_z_forward);
    const stale_doc_z_reverse = try internal_keys.relationalOrderedTupleIndexByDocKeyAlloc(alloc, "doc:z", "orders_status_amount_idx", stale_tuple);
    defer alloc.free(stale_doc_z_reverse);
    const amount_b_index = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "doc:b");
    defer alloc.free(amount_b_index);
    const amount_b_reverse = try internal_keys.relationalColumnIndexByDocKeyAlloc(alloc, "doc:b", "amount");
    defer alloc.free(amount_b_reverse);

    try store.putBatch(&.{
        .{ .key = stale_doc_a_forward, .value = "" },
        .{ .key = stale_doc_z_forward, .value = "" },
        .{ .key = stale_doc_z_reverse, .value = "" },
    }, &.{ doc_b_forward, doc_b_reverse, amount_b_index, amount_b_reverse });

    try std.testing.expectError(error.NotFound, store.get(alloc, doc_b_forward));
    const stale_before = try store.get(alloc, stale_doc_a_forward);
    defer alloc.free(stale_before);

    const report = try repairColumnBackedIndexesFromRowsInRangeWithColumnIndexPolicy(alloc, &store, "doc:a", "doc:zz", policy);
    try std.testing.expectEqual(@as(u64, 2), report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 2), report.indexed_rows);
    try std.testing.expect(report.deleted_orphan_entries >= 1);
    try std.testing.expect(report.written_entries >= 6);

    const repaired_a_forward = try store.get(alloc, doc_a_forward);
    defer alloc.free(repaired_a_forward);
    const repaired_a_reverse = try store.get(alloc, doc_a_reverse);
    defer alloc.free(repaired_a_reverse);
    const repaired_b_forward = try store.get(alloc, doc_b_forward);
    defer alloc.free(repaired_b_forward);
    const repaired_b_reverse = try store.get(alloc, doc_b_reverse);
    defer alloc.free(repaired_b_reverse);
    const repaired_amount_b = try store.get(alloc, amount_b_index);
    defer alloc.free(repaired_amount_b);
    const repaired_amount_b_reverse = try store.get(alloc, amount_b_reverse);
    defer alloc.free(repaired_amount_b_reverse);

    try std.testing.expectError(error.NotFound, store.get(alloc, stale_doc_a_forward));
    try std.testing.expectError(error.NotFound, store.get(alloc, stale_doc_z_forward));
    try std.testing.expectError(error.NotFound, store.get(alloc, stale_doc_z_reverse));
}
