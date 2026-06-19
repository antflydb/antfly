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

const std = @import("std");
const sql_adapter = @This();

const diagnostics = @import("diagnostics.zig");
const value_mod = @import("value.zig");

pub const SqlValue = value_mod.SqlValue;

pub const UnsupportedPlanFamily = enum {
    query,
    read,
    ddl,
    write,
    insert,
    update,
    update_source,
    delete,
    update_joined_source,
    delete_joined_source,
    merge_mutation,
};

pub const AppParityCorpusPlanFamily = enum {
    ddl,
    read,
    query,
    aggregate,
    join,
    lateral,
    window,
    explain,
    relation_population,
    insert,
    insert_source,
    update,
    delete,
    update_source,
    delete_source,
    truncate_source,
    update_joined_source,
    delete_joined_source,
    merge_mutation,
    adapter_noop_ddl,
    unsupported,
    unsupported_read,
    unsupported_ddl,
    unsupported_write,
    unsupported_insert,
    unsupported_update,
    unsupported_update_source,
    unsupported_delete,
    unsupported_update_joined_source,
    unsupported_delete_joined_source,
    unsupported_merge_mutation,
};

pub const AppParityDdlTag = enum {
    create_table,
    table_clone,
    create_view,
    rename_view,
    drop_view,
    create_materialized_view,
    refresh_materialized_view,
    drop_materialized_view,
    relation_lifetime,
    create_enum_type,
    add_enum_value,
    drop_enum_type,
    create_domain,
    alter_domain,
    drop_domain,
    create_sequence,
    alter_sequence,
    drop_sequence,
    identity_allocator,
    create_schema_namespace,
    rename_schema_namespace,
    drop_schema_namespace,
    create_extension,
    alter_extension_update,
    drop_extension,
    create_function,
    drop_function,
    create_procedure,
    drop_procedure,
    create_role,
    alter_role,
    drop_role,
    grant_privilege,
    revoke_privilege,
    copy_from,
    copy_to,
    create_partitioned_table,
    create_table_partition,
    attach_table_partition,
    detach_table_partition,
    enable_row_security,
    disable_row_security,
    create_row_policy,
    drop_row_policy,
    create_database,
    alter_database,
    drop_database,
    create_tablespace,
    rename_tablespace,
    drop_tablespace,
    listen_notification,
    notify_notification,
    unlisten_notification,
    create_publication,
    alter_publication,
    drop_publication,
    create_subscription,
    alter_subscription,
    drop_subscription,
    create_collation,
    rename_collation,
    drop_collation,
    create_operator,
    drop_operator,
    create_aggregate,
    drop_aggregate,
    create_cast,
    drop_cast,
    vacuum_maintenance,
    analyze_maintenance,
    reindex_maintenance,
    cluster_maintenance,
    prepare_statement,
    execute_statement,
    deallocate_statement,
    declare_cursor,
    fetch_cursor,
    close_cursor,
    savepoint_transaction,
    release_savepoint,
    rollback_to_savepoint,
    comment_metadata,
    table_lock,
    constraint_mode,
    transaction_mode,
    advisory_lock,
    set_search_path,
    reset_search_path,
    show_search_path,
    discard_all,
    create_index,
    drop_index,
    drop_table,
    alter_table,
    create_update_policy,
};

pub const AppParityPlanSummary = struct {
    ddl_tag: ?AppParityDdlTag = null,
    table_name: ?[]const u8 = null,
    ctes: ?usize = null,
    predicates: ?usize = null,
    array_any: ?usize = null,
    in_predicates: ?usize = null,
    json_path_eq: ?usize = null,
    json_contains: ?usize = null,
    json_path_exists: ?usize = null,
    array_contains: ?usize = null,
    array_eq: ?usize = null,
    text_patterns: ?usize = null,
    access_or_predicates: ?usize = null,
    access_not_predicates: ?usize = null,
    expression_predicates: ?usize = null,
    expression_or_predicates: ?usize = null,
    expression_not_predicates: ?usize = null,
    expression_array_contains: ?usize = null,
    select: ?usize = null,
    select_all: ?bool = null,
    distinct_on: ?usize = null,
    order_by: ?usize = null,
    limit: ?u32 = null,
    offset: ?u32 = null,
    right_offset: ?u32 = null,
    group_by: ?usize = null,
    group_expressions: ?usize = null,
    aggregations: ?usize = null,
    filter_groups: ?usize = null,
    having: ?usize = null,
    having_expressions: ?usize = null,
    having_any: ?usize = null,
    having_not: ?usize = null,
    operations: ?usize = null,
    source_assignments: ?usize = null,
    patch_expressions: ?usize = null,
    increment_expressions: ?usize = null,
    json_set_expressions: ?usize = null,
    returning: ?usize = null,
    returning_all: ?bool = null,
    conflict_where: ?bool = null,
    join_on: ?usize = null,
    matched_predicates: ?usize = null,
    matched_delete: ?bool = null,
    matched_do_nothing: ?bool = null,
    not_matched_predicates: ?usize = null,
    not_matched_do_nothing: ?bool = null,
    join_select: ?usize = null,
    lateral_correlations: ?usize = null,
    windows: ?usize = null,
    row_claim_skip_locked: ?bool = null,
    temporal_periods: ?usize = null,
    temporal_primary_key: ?bool = null,
    temporal_unique: ?usize = null,
    temporal_foreign_keys: ?usize = null,
};

pub fn summaryHasFields(summary: AppParityPlanSummary) bool {
    return summary.ddl_tag != null or
        summary.table_name != null or
        summary.ctes != null or
        summary.predicates != null or
        summary.array_any != null or
        summary.in_predicates != null or
        summary.json_path_eq != null or
        summary.json_contains != null or
        summary.json_path_exists != null or
        summary.array_contains != null or
        summary.array_eq != null or
        summary.text_patterns != null or
        summary.access_or_predicates != null or
        summary.access_not_predicates != null or
        summary.expression_predicates != null or
        summary.expression_or_predicates != null or
        summary.expression_not_predicates != null or
        summary.expression_array_contains != null or
        summary.select != null or
        summary.select_all != null or
        summary.distinct_on != null or
        summary.order_by != null or
        summary.limit != null or
        summary.offset != null or
        summary.right_offset != null or
        summary.group_by != null or
        summary.group_expressions != null or
        summary.aggregations != null or
        summary.filter_groups != null or
        summary.having != null or
        summary.having_expressions != null or
        summary.having_any != null or
        summary.having_not != null or
        summary.operations != null or
        summary.source_assignments != null or
        summary.patch_expressions != null or
        summary.increment_expressions != null or
        summary.json_set_expressions != null or
        summary.returning != null or
        summary.returning_all != null or
        summary.conflict_where != null or
        summary.join_on != null or
        summary.matched_predicates != null or
        summary.matched_delete != null or
        summary.matched_do_nothing != null or
        summary.not_matched_predicates != null or
        summary.not_matched_do_nothing != null or
        summary.join_select != null or
        summary.lateral_correlations != null or
        summary.windows != null or
        summary.row_claim_skip_locked != null or
        summary.temporal_periods != null or
        summary.temporal_primary_key != null or
        summary.temporal_unique != null or
        summary.temporal_foreign_keys != null;
}

pub fn summaryHasNonTableFields(summary: AppParityPlanSummary) bool {
    var without_table = summary;
    without_table.table_name = null;
    return summaryHasFields(without_table);
}

pub const AppParityCorpusEntry = struct {
    name: []const u8,
    sql: []const u8,
    family: AppParityCorpusPlanFamily,
    params: []const SqlValue = &.{},
    summary: AppParityPlanSummary = .{},
    plan: []const u8 = "",
    classification_reason: []const u8 = "",
    apply_setup_sql: []const []const u8 = &.{},
    returning_rows: []const []const u8 = &.{},
    applied_plan: []const u8 = "",
    resolver_row_json: []const u8 = "",
    resolver_version: u64 = 0,
    resolver_exists: ?bool = null,
    source_schema_json: []const u8 = "",
};

pub const app_parity_fixture_format: u64 = 1;
pub const app_parity_coverage_fixture_format: u64 = 1;
pub const app_parity_source_corpus_format: u64 = 1;

pub const AppParityFixtureRoot = struct {
    fixture_format: u64,
    source_entry_count: usize,
    entry_count: usize,
    skipped_entries: []const []const u8,
    schema_json: []const u8,
    entries: []const std.json.Value,
};

pub const AppParitySourceCorpusRoot = struct {
    source_format: u64,
    entries: []const AppParityCorpusEntry,
};

pub const AppParityFixtureEncodedEntry = struct {
    entry: AppParityCorpusEntry,
    applied_plan: []const u8 = "",
};

pub const AppParityFixtureGateMode = union(enum) {
    none,
    check: []const u8,
    promote: []const u8,
};

pub fn fixtureJsonObject(value: std.json.Value) !std.json.ObjectMap {
    return switch (value) {
        .object => |object| object,
        else => error.TestUnexpectedResult,
    };
}

fn fixtureStringIn(field: []const u8, allowed: []const []const u8) bool {
    for (allowed) |item| {
        if (std.mem.eql(u8, field, item)) return true;
    }
    return false;
}

pub fn fixtureRequireOnlyKeys(object: std.json.ObjectMap, allowed: []const []const u8) !void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (!fixtureStringIn(entry.key_ptr.*, allowed)) return error.TestUnexpectedResult;
    }
}

pub fn fixtureJsonString(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |text| text,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalString(object: std.json.ObjectMap, field: []const u8, default: []const u8) ![]const u8 {
    return if (object.get(field)) |value| try fixtureJsonString(value) else default;
}

pub fn fixtureJsonOptionalStringField(object: std.json.ObjectMap, field: []const u8) !?[]const u8 {
    return if (object.get(field)) |value| try fixtureJsonString(value) else null;
}

pub fn fixtureJsonOptionalBool(object: std.json.ObjectMap, field: []const u8) !?bool {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .bool => |flag| flag,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalUsize(object: std.json.ObjectMap, field: []const u8) !?usize {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalU32(object: std.json.ObjectMap, field: []const u8) !?u32 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .integer => |number| if (number >= 0 and number <= std.math.maxInt(u32)) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn fixtureJsonOptionalU64(object: std.json.ObjectMap, field: []const u8, default: u64) !u64 {
    const value = object.get(field) orelse return default;
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.TestUnexpectedResult,
        else => error.TestUnexpectedResult,
    };
}

pub fn parseFixtureRootAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParityFixtureRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "fixture_format", "source_entry_count", "entry_count", "skipped_entries", "schema_json", "entries" });
    const fixture_format = try fixtureJsonOptionalU64(root, "fixture_format", 0);
    if (fixture_format != app_parity_fixture_format) return error.TestUnexpectedResult;
    const source_entry_count = try fixtureJsonOptionalUsize(root, "source_entry_count") orelse return error.TestUnexpectedResult;
    const entry_count = try fixtureJsonOptionalUsize(root, "entry_count") orelse return error.TestUnexpectedResult;
    const skipped_entries = try parseFixtureStringListAlloc(alloc, root, "skipped_entries");
    errdefer if (skipped_entries.len > 0) alloc.free(skipped_entries);
    const schema_json = try fixtureJsonString(root.get("schema_json") orelse return error.TestUnexpectedResult);
    const entries = switch (root.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entries.len == 0) return error.TestUnexpectedResult;
    if (entry_count != entries.len) return error.TestUnexpectedResult;
    if (source_entry_count != entries.len + skipped_entries.len) return error.TestUnexpectedResult;
    return .{
        .fixture_format = fixture_format,
        .source_entry_count = source_entry_count,
        .entry_count = entry_count,
        .skipped_entries = skipped_entries,
        .schema_json = schema_json,
        .entries = entries,
    };
}

pub fn freeFixtureRoot(alloc: std.mem.Allocator, root: AppParityFixtureRoot) void {
    if (root.skipped_entries.len > 0) alloc.free(root.skipped_entries);
}

pub fn parseFixtureSummary(value: ?std.json.Value) !AppParityPlanSummary {
    if (value == null) return .{};
    const object = try fixtureJsonObject(value.?);
    try fixtureRequireOnlyKeys(object, &.{
        "ddl_tag",
        "table_name",
        "ctes",
        "predicates",
        "array_any",
        "in_predicates",
        "json_path_eq",
        "json_contains",
        "json_path_exists",
        "array_contains",
        "array_eq",
        "text_patterns",
        "access_or_predicates",
        "access_not_predicates",
        "expression_predicates",
        "expression_or_predicates",
        "expression_not_predicates",
        "expression_array_contains",
        "select",
        "select_all",
        "distinct_on",
        "order_by",
        "limit",
        "offset",
        "right_offset",
        "group_by",
        "group_expressions",
        "aggregations",
        "filter_groups",
        "having",
        "having_expressions",
        "having_any",
        "having_not",
        "operations",
        "source_assignments",
        "patch_expressions",
        "increment_expressions",
        "json_set_expressions",
        "returning",
        "returning_all",
        "conflict_where",
        "join_on",
        "matched_predicates",
        "matched_delete",
        "matched_do_nothing",
        "not_matched_predicates",
        "not_matched_do_nothing",
        "join_select",
        "lateral_correlations",
        "windows",
        "row_claim_skip_locked",
        "temporal_periods",
        "temporal_primary_key",
        "temporal_unique",
        "temporal_foreign_keys",
    });
    return .{
        .ddl_tag = if (object.get("ddl_tag")) |tag_value| std.meta.stringToEnum(AppParityDdlTag, try fixtureJsonString(tag_value)) orelse return error.TestUnexpectedResult else null,
        .table_name = try fixtureJsonOptionalStringField(object, "table_name"),
        .ctes = try fixtureJsonOptionalUsize(object, "ctes"),
        .predicates = try fixtureJsonOptionalUsize(object, "predicates"),
        .array_any = try fixtureJsonOptionalUsize(object, "array_any"),
        .in_predicates = try fixtureJsonOptionalUsize(object, "in_predicates"),
        .json_path_eq = try fixtureJsonOptionalUsize(object, "json_path_eq"),
        .json_contains = try fixtureJsonOptionalUsize(object, "json_contains"),
        .json_path_exists = try fixtureJsonOptionalUsize(object, "json_path_exists"),
        .array_contains = try fixtureJsonOptionalUsize(object, "array_contains"),
        .array_eq = try fixtureJsonOptionalUsize(object, "array_eq"),
        .text_patterns = try fixtureJsonOptionalUsize(object, "text_patterns"),
        .access_or_predicates = try fixtureJsonOptionalUsize(object, "access_or_predicates"),
        .access_not_predicates = try fixtureJsonOptionalUsize(object, "access_not_predicates"),
        .expression_predicates = try fixtureJsonOptionalUsize(object, "expression_predicates"),
        .expression_or_predicates = try fixtureJsonOptionalUsize(object, "expression_or_predicates"),
        .expression_not_predicates = try fixtureJsonOptionalUsize(object, "expression_not_predicates"),
        .expression_array_contains = try fixtureJsonOptionalUsize(object, "expression_array_contains"),
        .select = try fixtureJsonOptionalUsize(object, "select"),
        .select_all = try fixtureJsonOptionalBool(object, "select_all"),
        .distinct_on = try fixtureJsonOptionalUsize(object, "distinct_on"),
        .order_by = try fixtureJsonOptionalUsize(object, "order_by"),
        .limit = try fixtureJsonOptionalU32(object, "limit"),
        .offset = try fixtureJsonOptionalU32(object, "offset"),
        .right_offset = try fixtureJsonOptionalU32(object, "right_offset"),
        .group_by = try fixtureJsonOptionalUsize(object, "group_by"),
        .group_expressions = try fixtureJsonOptionalUsize(object, "group_expressions"),
        .aggregations = try fixtureJsonOptionalUsize(object, "aggregations"),
        .filter_groups = try fixtureJsonOptionalUsize(object, "filter_groups"),
        .having = try fixtureJsonOptionalUsize(object, "having"),
        .having_expressions = try fixtureJsonOptionalUsize(object, "having_expressions"),
        .having_any = try fixtureJsonOptionalUsize(object, "having_any"),
        .having_not = try fixtureJsonOptionalUsize(object, "having_not"),
        .operations = try fixtureJsonOptionalUsize(object, "operations"),
        .source_assignments = try fixtureJsonOptionalUsize(object, "source_assignments"),
        .patch_expressions = try fixtureJsonOptionalUsize(object, "patch_expressions"),
        .increment_expressions = try fixtureJsonOptionalUsize(object, "increment_expressions"),
        .json_set_expressions = try fixtureJsonOptionalUsize(object, "json_set_expressions"),
        .returning = try fixtureJsonOptionalUsize(object, "returning"),
        .returning_all = try fixtureJsonOptionalBool(object, "returning_all"),
        .conflict_where = try fixtureJsonOptionalBool(object, "conflict_where"),
        .join_on = try fixtureJsonOptionalUsize(object, "join_on"),
        .matched_predicates = try fixtureJsonOptionalUsize(object, "matched_predicates"),
        .matched_delete = try fixtureJsonOptionalBool(object, "matched_delete"),
        .matched_do_nothing = try fixtureJsonOptionalBool(object, "matched_do_nothing"),
        .not_matched_predicates = try fixtureJsonOptionalUsize(object, "not_matched_predicates"),
        .not_matched_do_nothing = try fixtureJsonOptionalBool(object, "not_matched_do_nothing"),
        .join_select = try fixtureJsonOptionalUsize(object, "join_select"),
        .lateral_correlations = try fixtureJsonOptionalUsize(object, "lateral_correlations"),
        .windows = try fixtureJsonOptionalUsize(object, "windows"),
        .row_claim_skip_locked = try fixtureJsonOptionalBool(object, "row_claim_skip_locked"),
        .temporal_periods = try fixtureJsonOptionalUsize(object, "temporal_periods"),
        .temporal_primary_key = try fixtureJsonOptionalBool(object, "temporal_primary_key"),
        .temporal_unique = try fixtureJsonOptionalUsize(object, "temporal_unique"),
        .temporal_foreign_keys = try fixtureJsonOptionalUsize(object, "temporal_foreign_keys"),
    };
}

pub fn parseFixtureStringListAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    field: []const u8,
) ![]const []const u8 {
    const value = object.get(field) orelse return &.{};
    const array = switch (value) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    var strings = std.ArrayListUnmanaged([]const u8).empty;
    errdefer strings.deinit(alloc);
    for (array.items) |item| {
        try strings.append(alloc, try fixtureJsonString(item));
    }
    return try strings.toOwnedSlice(alloc);
}

pub fn parseFixtureSqlValue(value: std.json.Value) !SqlValue {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{ "null", "bool", "integer", "float", "string", "json" });
    var it = object.iterator();
    const entry = it.next() orelse return error.TestUnexpectedResult;
    if (it.next() != null) return error.TestUnexpectedResult;
    if (std.mem.eql(u8, entry.key_ptr.*, "null")) return .null;
    if (std.mem.eql(u8, entry.key_ptr.*, "bool")) {
        return switch (entry.value_ptr.*) {
            .bool => |flag| .{ .bool = flag },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "integer")) {
        return switch (entry.value_ptr.*) {
            .integer => |number| .{ .integer = number },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "float")) {
        return switch (entry.value_ptr.*) {
            .integer => |number| .{ .float = @floatFromInt(number) },
            .float => |number| .{ .float = number },
            else => error.TestUnexpectedResult,
        };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "string")) {
        return .{ .string = try fixtureJsonString(entry.value_ptr.*) };
    }
    if (std.mem.eql(u8, entry.key_ptr.*, "json")) {
        return .{ .json = try fixtureJsonString(entry.value_ptr.*) };
    }
    return error.TestUnexpectedResult;
}

pub fn parseFixtureSqlValuesAlloc(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
) ![]const SqlValue {
    const value = object.get("params") orelse return &.{};
    const array = switch (value) {
        .array => |items| items,
        else => return error.TestUnexpectedResult,
    };
    var params = std.ArrayListUnmanaged(SqlValue).empty;
    errdefer params.deinit(alloc);
    for (array.items) |item| {
        try params.append(alloc, try parseFixtureSqlValue(item));
    }
    return try params.toOwnedSlice(alloc);
}

pub fn parseFixtureEntryAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParityCorpusEntry {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{
        "name",
        "sql",
        "family",
        "params",
        "summary",
        "plan",
        "classification_reason",
        "apply_setup_sql",
        "returning_rows",
        "applied_plan",
        "resolver_row_json",
        "resolver_version",
        "resolver_exists",
        "source_schema_json",
    });
    const family_text = try fixtureJsonString(object.get("family") orelse return error.TestUnexpectedResult);
    const family = std.meta.stringToEnum(AppParityCorpusPlanFamily, family_text) orelse return error.TestUnexpectedResult;
    const plan = try fixtureJsonOptionalString(object, "plan", "");
    const summary = normalizeFixtureSummary(
        family,
        plan,
        try parseFixtureSummary(object.get("summary")),
    );
    return .{
        .name = try fixtureJsonString(object.get("name") orelse return error.TestUnexpectedResult),
        .sql = try fixtureJsonString(object.get("sql") orelse return error.TestUnexpectedResult),
        .family = family,
        .params = try parseFixtureSqlValuesAlloc(alloc, object),
        .summary = summary,
        .plan = plan,
        .classification_reason = try fixtureJsonOptionalString(object, "classification_reason", ""),
        .apply_setup_sql = try parseFixtureStringListAlloc(alloc, object, "apply_setup_sql"),
        .returning_rows = try parseFixtureStringListAlloc(alloc, object, "returning_rows"),
        .applied_plan = try fixtureJsonOptionalString(object, "applied_plan", ""),
        .resolver_row_json = try fixtureJsonOptionalString(object, "resolver_row_json", ""),
        .resolver_version = try fixtureJsonOptionalU64(object, "resolver_version", 0),
        .resolver_exists = try fixtureJsonOptionalBool(object, "resolver_exists"),
        .source_schema_json = try fixtureJsonOptionalString(object, "source_schema_json", ""),
    };
}

pub fn parseSourceCorpusRootAlloc(alloc: std.mem.Allocator, value: std.json.Value) !AppParitySourceCorpusRoot {
    const root = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(root, &.{ "source_format", "entries" });
    const source_format = try fixtureJsonOptionalU64(root, "source_format", 0);
    if (source_format != app_parity_source_corpus_format) return error.TestUnexpectedResult;

    const entry_values = switch (root.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entry_values.len == 0) return error.TestUnexpectedResult;

    var entries = std.ArrayListUnmanaged(AppParityCorpusEntry).empty;
    errdefer {
        for (entries.items) |entry| freeFixtureEntry(alloc, entry);
        entries.deinit(alloc);
    }
    var seen_names = std.StringHashMapUnmanaged(void){};
    defer seen_names.deinit(alloc);

    for (entry_values) |entry_value| {
        const entry = try parseFixtureEntryAlloc(alloc, entry_value);
        errdefer freeFixtureEntry(alloc, entry);
        if (entry.name.len == 0 or seen_names.contains(entry.name)) return error.TestUnexpectedResult;
        try validateSourceCorpusEntryMetadata(entry);
        try validateSourceCorpusEntryJsonPayloads(alloc, entry);
        try seen_names.put(alloc, entry.name, {});
        try entries.append(alloc, entry);
    }

    return .{
        .source_format = source_format,
        .entries = try entries.toOwnedSlice(alloc),
    };
}

pub fn freeSourceCorpusRoot(alloc: std.mem.Allocator, root: AppParitySourceCorpusRoot) void {
    for (root.entries) |entry| freeFixtureEntry(alloc, entry);
    alloc.free(root.entries);
}

fn normalizeFixtureSummary(
    family: AppParityCorpusPlanFamily,
    plan: []const u8,
    summary: AppParityPlanSummary,
) AppParityPlanSummary {
    var normalized = summary;
    if (family == .update_joined_source and
        normalized.source_assignments == null and
        normalized.patch_expressions != null and
        planHasNonZeroToken(plan, ":source_assignments=") and
        !planHasNonZeroToken(plan, ":patch_expr="))
    {
        normalized.source_assignments = normalized.patch_expressions;
        normalized.patch_expressions = null;
    }
    return normalized;
}

pub fn freeFixtureEntry(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) void {
    if (entry.params.len > 0) alloc.free(entry.params);
    if (entry.apply_setup_sql.len > 0) alloc.free(entry.apply_setup_sql);
    if (entry.returning_rows.len > 0) alloc.free(entry.returning_rows);
}

fn appParityCoverageFlag(coverage: AppParityCorpusCoverage, name: []const u8) !bool {
    inline for (std.meta.fields(AppParityCorpusCoverage)) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            if (field.type != bool) return error.TestUnexpectedResult;
            return @field(coverage, field.name);
        }
    }
    return error.TestUnexpectedResult;
}

fn checkCoverageRegressionCase(alloc: std.mem.Allocator, value: std.json.Value) !void {
    const object = try fixtureJsonObject(value);
    try fixtureRequireOnlyKeys(object, &.{ "name", "entries", "expect_true", "expect_false" });

    const entries = switch (object.get("entries") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (entries.len == 0) return error.TestUnexpectedResult;

    const expect_true = try parseFixtureStringListAlloc(alloc, object, "expect_true");
    defer {
        if (expect_true.len > 0) alloc.free(expect_true);
    }
    const expect_false = try parseFixtureStringListAlloc(alloc, object, "expect_false");
    defer {
        if (expect_false.len > 0) alloc.free(expect_false);
    }
    if (expect_true.len == 0 and expect_false.len == 0) return error.TestUnexpectedResult;

    var coverage = AppParityCorpusCoverage{};
    for (entries) |entry_value| {
        const entry = try parseFixtureEntryAlloc(alloc, entry_value);
        defer freeFixtureEntry(alloc, entry);
        try coverage.observe(alloc, entry);
    }

    for (expect_true) |name| {
        try std.testing.expect(try appParityCoverageFlag(coverage, name));
    }
    for (expect_false) |name| {
        try std.testing.expect(!try appParityCoverageFlag(coverage, name));
    }
}

fn fixtureWriteObjectComma(writer: anytype, first: *bool) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeAll(",\n");
    }
}

fn fixtureWriteStringField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: []const u8) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {f}", .{ indent, std.json.fmt(name, .{}), std.json.fmt(value, .{}) });
}

fn fixtureWriteBoolField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: bool) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {}", .{ indent, std.json.fmt(name, .{}), value });
}

fn fixtureWriteU64Field(writer: anytype, first: *bool, indent: []const u8, name: []const u8, value: u64) !void {
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: {d}", .{ indent, std.json.fmt(name, .{}), value });
}

fn fixtureWriteUsizeSummaryField(writer: anytype, first: *bool, name: []const u8, value: ?usize) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {d}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteU32SummaryField(writer: anytype, first: *bool, name: []const u8, value: ?u32) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {d}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteBoolSummaryField(writer: anytype, first: *bool, name: []const u8, value: ?bool) !void {
    if (value) |actual| {
        try fixtureWriteObjectComma(writer, first);
        try writer.print("        {f}: {}", .{ std.json.fmt(name, .{}), actual });
    }
}

fn fixtureWriteStringListField(writer: anytype, first: *bool, indent: []const u8, name: []const u8, values: []const []const u8) !void {
    if (values.len == 0) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}{f}: [", .{ indent, std.json.fmt(name, .{}) });
    for (values, 0..) |value, i| {
        if (i > 0) try writer.writeAll(", ");
        try writer.print("{f}", .{std.json.fmt(value, .{})});
    }
    try writer.writeByte(']');
}

fn fixtureWriteSqlValue(writer: anytype, value: SqlValue) !void {
    switch (value) {
        .null => try writer.writeAll("{\"null\": true}"),
        .bool => |actual| try writer.print("{{\"bool\": {}}}", .{actual}),
        .integer => |actual| try writer.print("{{\"integer\": {d}}}", .{actual}),
        .float => |actual| try writer.print("{{\"float\": {d}}}", .{actual}),
        .string => |actual| try writer.print("{{\"string\": {f}}}", .{std.json.fmt(actual, .{})}),
        .json => |actual| try writer.print("{{\"json\": {f}}}", .{std.json.fmt(actual, .{})}),
    }
}

fn fixtureWriteParamsField(writer: anytype, first: *bool, indent: []const u8, params: []const SqlValue) !void {
    if (params.len == 0) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.print("{s}\"params\": [", .{indent});
    for (params, 0..) |param, i| {
        if (i > 0) try writer.writeAll(", ");
        try fixtureWriteSqlValue(writer, param);
    }
    try writer.writeByte(']');
}

fn fixtureWriteSummaryField(writer: anytype, first: *bool, summary: AppParityPlanSummary) !void {
    if (!summaryHasFields(summary)) return;
    try fixtureWriteObjectComma(writer, first);
    try writer.writeAll("      \"summary\": {\n");
    var summary_first = true;
    if (summary.ddl_tag) |tag| try fixtureWriteStringField(writer, &summary_first, "        ", "ddl_tag", @tagName(tag));
    if (summary.table_name) |table_name| try fixtureWriteStringField(writer, &summary_first, "        ", "table_name", table_name);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "ctes", summary.ctes);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "predicates", summary.predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_any", summary.array_any);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "in_predicates", summary.in_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_path_eq", summary.json_path_eq);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_contains", summary.json_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_path_exists", summary.json_path_exists);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_contains", summary.array_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "array_eq", summary.array_eq);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "text_patterns", summary.text_patterns);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "access_or_predicates", summary.access_or_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "access_not_predicates", summary.access_not_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_predicates", summary.expression_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_or_predicates", summary.expression_or_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_not_predicates", summary.expression_not_predicates);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "expression_array_contains", summary.expression_array_contains);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "select", summary.select);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "select_all", summary.select_all);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "distinct_on", summary.distinct_on);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "order_by", summary.order_by);
    try fixtureWriteU32SummaryField(writer, &summary_first, "limit", summary.limit);
    try fixtureWriteU32SummaryField(writer, &summary_first, "offset", summary.offset);
    try fixtureWriteU32SummaryField(writer, &summary_first, "right_offset", summary.right_offset);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "group_by", summary.group_by);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "group_expressions", summary.group_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "aggregations", summary.aggregations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "filter_groups", summary.filter_groups);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having", summary.having);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_expressions", summary.having_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_any", summary.having_any);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "having_not", summary.having_not);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "operations", summary.operations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "source_assignments", summary.source_assignments);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "patch_expressions", summary.patch_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "increment_expressions", summary.increment_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "json_set_expressions", summary.json_set_expressions);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "returning", summary.returning);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "returning_all", summary.returning_all);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "conflict_where", summary.conflict_where);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "join_on", summary.join_on);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "matched_predicates", summary.matched_predicates);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "matched_delete", summary.matched_delete);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "matched_do_nothing", summary.matched_do_nothing);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "not_matched_predicates", summary.not_matched_predicates);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "not_matched_do_nothing", summary.not_matched_do_nothing);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "join_select", summary.join_select);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "lateral_correlations", summary.lateral_correlations);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "windows", summary.windows);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "row_claim_skip_locked", summary.row_claim_skip_locked);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_periods", summary.temporal_periods);
    try fixtureWriteBoolSummaryField(writer, &summary_first, "temporal_primary_key", summary.temporal_primary_key);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_unique", summary.temporal_unique);
    try fixtureWriteUsizeSummaryField(writer, &summary_first, "temporal_foreign_keys", summary.temporal_foreign_keys);
    try writer.writeAll("\n      }");
}

pub fn fixtureJsonAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    source_entry_count: usize,
    entries: []const AppParityFixtureEncodedEntry,
    skipped_entries: []const []const u8,
) ![]u8 {
    var entries_out: std.Io.Writer.Allocating = .init(alloc);
    errdefer entries_out.deinit();
    const entries_writer = &entries_out.writer;
    for (entries, 0..) |encoded, i| {
        const entry = encoded.entry;
        if (i > 0) try entries_writer.writeByte(',');
        try entries_writer.writeAll("\n    {\n");
        var first = true;
        try fixtureWriteStringField(entries_writer, &first, "      ", "name", entry.name);
        try fixtureWriteStringField(entries_writer, &first, "      ", "family", @tagName(entry.family));
        try fixtureWriteSummaryField(entries_writer, &first, entry.summary);
        try fixtureWriteStringField(entries_writer, &first, "      ", "plan", entry.plan);
        if (entry.classification_reason.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "classification_reason", entry.classification_reason);
        try fixtureWriteStringListField(entries_writer, &first, "      ", "apply_setup_sql", entry.apply_setup_sql);
        try fixtureWriteStringListField(entries_writer, &first, "      ", "returning_rows", entry.returning_rows);
        if (encoded.applied_plan.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "applied_plan", encoded.applied_plan);
        if (entry.resolver_row_json.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "resolver_row_json", entry.resolver_row_json);
        if (entry.resolver_version != 0) try fixtureWriteU64Field(entries_writer, &first, "      ", "resolver_version", entry.resolver_version);
        if (entry.resolver_exists) |exists| try fixtureWriteBoolField(entries_writer, &first, "      ", "resolver_exists", exists);
        if (entry.source_schema_json.len > 0) try fixtureWriteStringField(entries_writer, &first, "      ", "source_schema_json", entry.source_schema_json);
        try fixtureWriteParamsField(entries_writer, &first, "      ", entry.params);
        try fixtureWriteStringField(entries_writer, &first, "      ", "sql", entry.sql);
        try entries_writer.writeAll("\n    }");
    }
    const entries_json = try entries_out.toOwnedSlice();
    defer alloc.free(entries_json);

    var skipped_out: std.Io.Writer.Allocating = .init(alloc);
    errdefer skipped_out.deinit();
    const skipped_writer = &skipped_out.writer;
    for (skipped_entries, 0..) |name, i| {
        if (i > 0) try skipped_writer.writeByte(',');
        try skipped_writer.writeAll("\n    ");
        try skipped_writer.print("{f}", .{std.json.fmt(name, .{})});
    }
    const skipped_json = try skipped_out.toOwnedSlice();
    defer alloc.free(skipped_json);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print(
        "{{\n  \"fixture_format\": {},\n  \"source_entry_count\": {},\n  \"entry_count\": {},\n  \"skipped_entries\": [",
        .{ app_parity_fixture_format, source_entry_count, entries.len },
    );
    try writer.writeAll(skipped_json);
    try writer.print(
        "\n  ],\n  \"schema_json\": {f},\n  \"entries\": [",
        .{std.json.fmt(schema_json, .{})},
    );
    try writer.writeAll(entries_json);
    try writer.writeAll("\n  ]\n}\n");
    return try out.toOwnedSlice();
}

pub fn fixtureGateModeFromPaths(promote_path: ?[]const u8, check_path: ?[]const u8) !AppParityFixtureGateMode {
    if (promote_path != null and check_path != null) return error.TestUnexpectedResult;
    if (promote_path) |path| return .{ .promote = path };
    if (check_path) |path| return .{ .check = path };
    return .none;
}

fn fixtureGateEnvPathAlloc(alloc: std.mem.Allocator, name: []const u8) !?[]u8 {
    const view = std.testing.environ.block.view();
    for (view.slice) |entry| {
        const text = std.mem.span(entry);
        const eq = std.mem.indexOfScalar(u8, text, '=') orelse continue;
        if (std.mem.eql(u8, text[0..eq], name)) return try alloc.dupe(u8, text[eq + 1 ..]);
    }
    return null;
}

pub fn fixtureGateModeFromEnvAlloc(alloc: std.mem.Allocator) !AppParityFixtureGateMode {
    const promote_path = try fixtureGateEnvPathAlloc(alloc, "ANTFLY_SQL_API_PARITY_FIXTURE_PROMOTE");
    errdefer if (promote_path) |path| alloc.free(path);
    const check_path = try fixtureGateEnvPathAlloc(alloc, "ANTFLY_SQL_API_PARITY_FIXTURE_CHECK");
    errdefer if (check_path) |path| alloc.free(path);
    return fixtureGateModeFromPaths(promote_path, check_path);
}

pub fn freeFixtureGateMode(alloc: std.mem.Allocator, mode: AppParityFixtureGateMode) void {
    switch (mode) {
        .none => {},
        .check, .promote => |path| alloc.free(path),
    }
}

pub fn checkOrPromoteFixtureJson(
    alloc: std.mem.Allocator,
    mode: AppParityFixtureGateMode,
    encoded: []const u8,
) !void {
    switch (mode) {
        .none => return,
        .check => |path| {
            const existing = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(encoded.len + 1));
            defer alloc.free(existing);
            if (!std.mem.eql(u8, existing, encoded)) {
                std.debug.print("SQL/API parity fixture is stale: {s}\nrun `zig build sql-api-parity-fixture-promote` from zig/\n", .{path});
                return error.TestUnexpectedResult;
            }
        },
        .promote => |path| {
            var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
            defer file.close(std.testing.io);

            var file_buf: [4096]u8 = undefined;
            var writer = file.writer(std.testing.io, &file_buf);
            try writer.interface.writeAll(encoded);
            try writer.end();
        },
    }
}

pub fn corpusUnsupportedPlanFamily(family: AppParityCorpusPlanFamily) ?UnsupportedPlanFamily {
    return switch (family) {
        .unsupported => .query,
        .unsupported_read => .read,
        .unsupported_ddl => .ddl,
        .unsupported_write => .write,
        .unsupported_insert => .insert,
        .unsupported_update => .update,
        .unsupported_update_source => .update_source,
        .unsupported_delete => .delete,
        .unsupported_update_joined_source => .update_joined_source,
        .unsupported_delete_joined_source => .delete_joined_source,
        .unsupported_merge_mutation => .merge_mutation,
        else => null,
    };
}

pub fn corpusPlanFamilyIsUnsupported(family: AppParityCorpusPlanFamily) bool {
    return corpusUnsupportedPlanFamily(family) != null;
}

pub fn corpusFixtureFamilyNeedsReason(family: AppParityCorpusPlanFamily) bool {
    return family == .adapter_noop_ddl or corpusPlanFamilyIsUnsupported(family);
}

pub fn corpusStableReasonToken(reason: []const u8) bool {
    return diagnostics.classificationReasonTokenIsKnown(reason);
}

pub fn corpusPlanMatchesReason(
    family: AppParityCorpusPlanFamily,
    plan: []const u8,
    reason: []const u8,
) bool {
    const diagnostic_reason = diagnostics.classificationReasonFromToken(reason) orelse return false;
    switch (family) {
        .adapter_noop_ddl => return adapterNoopPlanMatchesReason(plan, "ddl", diagnostic_reason),
        else => if (corpusUnsupportedPlanFamily(family)) |unsupported_family| {
            return unsupportedPlanMatchesReason(plan, unsupported_family, diagnostic_reason);
        } else return true,
    }
}

pub fn corpusPlanMatchesFamily(family: AppParityCorpusPlanFamily, plan: []const u8) bool {
    if (corpusUnsupportedPlanFamily(family)) |unsupported_family| {
        return unsupportedPlanMatchesFamily(plan, unsupported_family);
    }

    const prefix = switch (family) {
        .ddl => "ddl:",
        .read => "read:",
        .query => "query:",
        .aggregate => "aggregate:",
        .join => "join:",
        .lateral => "lateral:",
        .window => "window:",
        .explain => "explain:",
        .relation_population => "relation_population:",
        .insert => "insert:",
        .insert_source => "insert_source:",
        .update => "update:",
        .delete => "delete:",
        .update_source => "update_source:",
        .delete_source => "delete_source:",
        .truncate_source => "truncate_source:",
        .update_joined_source => "update_joined_source:",
        .delete_joined_source => "delete_joined_source:",
        .merge_mutation => "merge_mutation:",
        .adapter_noop_ddl => "adapter_noop:ddl:",
        .unsupported,
        .unsupported_read,
        .unsupported_ddl,
        .unsupported_write,
        .unsupported_insert,
        .unsupported_update,
        .unsupported_update_source,
        .unsupported_delete,
        .unsupported_update_joined_source,
        .unsupported_delete_joined_source,
        .unsupported_merge_mutation,
        => unreachable,
    };
    return std.mem.startsWith(u8, plan, prefix);
}

pub fn corpusFixtureFamilyNeedsTableSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .explain,
        .relation_population,
        .insert,
        .insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .ddl,
        .read,
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .explain,
        .relation_population,
        .insert,
        .insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSourceSchema(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .read,
        .join,
        .lateral,
        .insert_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsSetupSql(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .unsupported_ddl,
        .adapter_noop_ddl,
        => false,
        else => true,
    };
}

pub fn corpusFixtureFamilyAllowsReturningRows(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .insert,
        .update,
        .delete,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsResolverHint(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .insert,
        .update,
        .delete,
        .unsupported_insert,
        .unsupported_update,
        .unsupported_delete,
        => true,
        else => false,
    };
}

pub fn corpusFixtureFamilyAllowsOperationsSummary(family: AppParityCorpusPlanFamily) bool {
    return switch (family) {
        .ddl,
        .insert,
        .insert_source,
        .update,
        .update_source,
        .update_joined_source,
        .merge_mutation,
        => true,
        else => false,
    };
}

pub fn corpusExplainWriteInnerHasPrefix(entry: AppParityCorpusEntry, inner_prefix: []const u8) bool {
    const inner_token = ":inner=";
    if (!std.mem.startsWith(u8, inner_prefix, inner_token)) return false;
    return entry.family == .explain and
        explainPlanHasKind(entry.plan, "write") and
        explainPlanInnerStartsWith(entry.plan, inner_prefix[inner_token.len..]);
}

pub fn corpusReadPlanHasPrefix(entry: AppParityCorpusEntry, read_prefix: []const u8) bool {
    return (entry.family == .read and std.mem.startsWith(u8, entry.plan, read_prefix)) or
        (entry.family == .explain and
            explainPlanHasKind(entry.plan, "read") and
            explainPlanInnerStartsWith(entry.plan, read_prefix));
}

pub fn corpusOptionalZeroSummaryMatchesPlan(plan_text: []const u8, token_text: []const u8, expected: usize) bool {
    return switch (scanUsizeToken(plan_text, token_text)) {
        .value => |value| value == expected,
        .absent => expected == 0,
        .invalid => false,
    };
}

pub fn corpusOptionalBool01SummaryMatchesPlan(plan_text: []const u8, token_text: []const u8, expected: bool) bool {
    const value = planUsizeTokenValue(plan_text, token_text) orelse return !expected;
    return value == @intFromBool(expected);
}

pub fn corpusFixtureHasAccessSummary(summary: AppParityPlanSummary) bool {
    return summary.array_any != null or
        summary.in_predicates != null or
        summary.json_path_eq != null or
        summary.json_contains != null or
        summary.json_path_exists != null or
        summary.array_contains != null or
        summary.array_eq != null or
        summary.text_patterns != null or
        summary.access_or_predicates != null or
        summary.access_not_predicates != null or
        summary.expression_predicates != null or
        summary.expression_or_predicates != null or
        summary.expression_not_predicates != null or
        summary.expression_array_contains != null;
}

pub fn corpusFixtureHasTemporalDdlSummary(entry: AppParityCorpusEntry) bool {
    return entry.summary.temporal_periods != null or
        entry.summary.temporal_primary_key != null or
        entry.summary.temporal_unique != null or
        entry.summary.temporal_foreign_keys != null;
}

pub fn corpusFixturePlanMatchesSourceTable(entry: AppParityCorpusEntry, source_table_name: []const u8) bool {
    return switch (entry.family) {
        .insert_source => planHasExactStringToken(entry.plan, ":source_table=", source_table_name),
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => planHasExactStringToken(entry.plan, ":source=", source_table_name),
        .read,
        .join,
        .lateral,
        => planHasExactStringToken(entry.plan, ":right=", source_table_name),
        else => false,
    };
}

pub fn corpusFixtureSqlParameterCoverageMatches(entry: AppParityCorpusEntry) bool {
    if (entry.family == .ddl and entry.summary.ddl_tag == .prepare_statement) {
        if (entry.params.len != 0) return false;
        const prepared_params = planUsizeTokenValue(entry.plan, ":params=") orelse return false;
        return sqlParameterCoverageMatches(entry.sql, prepared_params);
    }
    return sqlParameterCoverageMatches(entry.sql, entry.params.len);
}

pub fn corpusDdlFixtureRequiresAppliedPlan(entry: AppParityCorpusEntry) !bool {
    if (entry.family != .ddl) return false;
    return switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
        .drop_table => true,
        .create_view, .rename_view, .drop_view => false,
        .create_materialized_view, .refresh_materialized_view, .drop_materialized_view => false,
        .relation_lifetime => false,
        .create_enum_type, .add_enum_value, .drop_enum_type => false,
        .create_domain, .alter_domain, .drop_domain => false,
        .create_sequence, .alter_sequence, .drop_sequence => false,
        .identity_allocator => false,
        .create_schema_namespace, .rename_schema_namespace, .drop_schema_namespace => false,
        .create_extension, .alter_extension_update, .drop_extension => false,
        .create_function, .drop_function, .create_procedure, .drop_procedure => false,
        .create_role, .alter_role, .drop_role, .grant_privilege, .revoke_privilege => false,
        .copy_from, .copy_to => false,
        .create_partitioned_table, .create_table_partition, .attach_table_partition, .detach_table_partition => false,
        .enable_row_security, .disable_row_security, .create_row_policy, .drop_row_policy => false,
        .create_database, .alter_database, .drop_database => false,
        .create_tablespace, .rename_tablespace, .drop_tablespace => false,
        .listen_notification, .notify_notification, .unlisten_notification => false,
        .create_publication, .alter_publication, .drop_publication => false,
        .create_subscription, .alter_subscription, .drop_subscription => false,
        .create_collation, .rename_collation, .drop_collation => false,
        .create_operator, .drop_operator => false,
        .create_aggregate, .drop_aggregate => false,
        .create_cast, .drop_cast => false,
        .vacuum_maintenance, .analyze_maintenance, .reindex_maintenance, .cluster_maintenance => false,
        .prepare_statement, .execute_statement, .deallocate_statement => false,
        .declare_cursor, .fetch_cursor, .close_cursor => false,
        .savepoint_transaction, .release_savepoint, .rollback_to_savepoint => false,
        .set_search_path, .reset_search_path, .show_search_path, .discard_all => false,
        .comment_metadata => true,
        .table_lock, .constraint_mode, .transaction_mode, .advisory_lock => false,
        .create_table,
        .table_clone,
        .create_index,
        .drop_index,
        .alter_table,
        .create_update_policy,
        => true,
    };
}

pub fn corpusDdlFixtureAppliesFromEmptyCatalog(entry: AppParityCorpusEntry) !bool {
    if (entry.family != .ddl) return false;
    return switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
        .create_table => !planHasExactBoolToken(entry.plan, ":if_not_exists=", true) and
            !planHasExactBoolToken(entry.plan, ":replace=", true),
        else => false,
    };
}

const AppParityCorpusMetadataMode = enum {
    source,
    generated_fixture,
};

fn validateCorpusMetadataCore(entry: AppParityCorpusEntry, mode: AppParityCorpusMetadataMode) !void {
    if (entry.name.len == 0 or entry.sql.len == 0 or entry.plan.len == 0) return error.TestUnexpectedResult;
    if (!corpusPlanMatchesFamily(entry.family, entry.plan)) return error.TestUnexpectedResult;
    if (corpusFixtureFamilyNeedsReason(entry.family) and entry.classification_reason.len == 0) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureFamilyNeedsReason(entry.family) and entry.classification_reason.len > 0) {
        return error.TestUnexpectedResult;
    }
    if (entry.classification_reason.len > 0 and !corpusStableReasonToken(entry.classification_reason)) {
        return error.TestUnexpectedResult;
    }
    if (corpusFixtureFamilyNeedsReason(entry.family) and
        !corpusPlanMatchesReason(entry.family, entry.plan, entry.classification_reason))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.family == .ddl and entry.summary.ddl_tag == null) return error.TestUnexpectedResult;
    if (entry.summary.ddl_tag != null and entry.family != .ddl) return error.TestUnexpectedResult;
    if (corpusFixtureFamilyNeedsTableSummary(entry.family) and entry.summary.table_name == null) {
        return error.TestUnexpectedResult;
    }
    if (summaryHasFields(entry.summary) and !corpusFixtureFamilyAllowsSummary(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.family == .relation_population and summaryHasNonTableFields(entry.summary)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.ctes) |ctes| {
        if (!planHasExactUsizeToken(entry.plan, ":ctes=", ctes)) return error.TestUnexpectedResult;
    }
    if (entry.summary.operations != null and !corpusFixtureFamilyAllowsOperationsSummary(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.operations) |operations| {
        if (!corpusFixtureOperationsSummaryMatchesPlan(entry, operations)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.returning != null or entry.summary.returning_all != null) and
        !corpusFixtureAllowsReturningSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.returning) |returning| {
        if (!corpusFixtureReturningSummaryMatchesPlan(entry, returning)) return error.TestUnexpectedResult;
    }
    if (entry.summary.returning_all) |returning_all| {
        if (!corpusFixtureReturningAllSummaryMatchesPlan(entry, returning_all)) return error.TestUnexpectedResult;
    }
    if (entry.summary.conflict_where != null and !corpusFixtureAllowsConflictWhereSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.conflict_where) |conflict_where| {
        if (!corpusFixtureConflictWhereSummaryMatchesPlan(entry, conflict_where)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.patch_expressions != null or
        entry.summary.increment_expressions != null or
        entry.summary.json_set_expressions != null) and
        !corpusFixtureAllowsMutationTransformSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureTransformSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.source_assignments != null and !corpusFixtureAllowsSourceAssignmentsSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.source_assignments) |source_assignments| {
        if (!corpusFixtureSourceAssignmentsSummaryMatchesPlan(entry, source_assignments)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.matched_predicates != null or
        entry.summary.matched_delete != null or
        entry.summary.matched_do_nothing != null or
        entry.summary.not_matched_predicates != null or
        entry.summary.not_matched_do_nothing != null) and
        !corpusFixtureAllowsMergeArmSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureMergeArmSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if ((entry.summary.group_by != null or
        entry.summary.group_expressions != null or
        entry.summary.aggregations != null or
        entry.summary.filter_groups != null or
        entry.summary.having != null or
        entry.summary.having_expressions != null or
        entry.summary.having_any != null or
        entry.summary.having_not != null) and
        !corpusFixtureAllowsAggregateSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureAggregateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureDdlSelectSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureDdlPredicateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.predicates != null and !corpusFixtureAllowsPredicateSummary(entry)) return error.TestUnexpectedResult;
    if (!corpusFixturePredicateSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (corpusFixtureHasAccessSummary(entry.summary) and !corpusFixtureAllowsAccessSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureAccessSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (!corpusFixtureSelectSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.join_select != null and !corpusFixtureAllowsJoinSelectSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.join_select) |join_select| {
        if (!corpusFixtureJoinSelectSummaryMatchesPlan(entry, join_select)) return error.TestUnexpectedResult;
    }
    if (entry.summary.join_on != null and !corpusFixtureAllowsJoinOnSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (entry.summary.join_on) |join_on| {
        if (!corpusFixtureJoinOnSummaryMatchesPlan(entry, join_on)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.lateral_correlations != null or entry.summary.right_offset != null) and
        !corpusFixtureAllowsLateralSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureLateralSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.windows != null and !corpusFixtureAllowsWindowSummary(entry)) return error.TestUnexpectedResult;
    if (entry.summary.windows) |windows| {
        if (!corpusFixtureWindowSummaryMatchesPlan(entry, windows)) return error.TestUnexpectedResult;
    }
    if ((entry.summary.select_all != null or entry.summary.distinct_on != null) and
        !corpusFixtureAllowsFullQueryOutputSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureFullQueryOutputSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if ((entry.summary.order_by != null or entry.summary.limit != null or entry.summary.offset != null) and
        !corpusFixtureAllowsPaginationSummary(entry))
    {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixturePaginationSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (entry.summary.row_claim_skip_locked != null and !corpusFixtureAllowsRowClaimSummary(entry)) {
        return error.TestUnexpectedResult;
    }
    if (!corpusFixtureRowClaimSummaryMatchesPlan(entry)) return error.TestUnexpectedResult;
    if (corpusFixtureHasTemporalDdlSummary(entry)) {
        if (entry.family != .ddl) return error.TestUnexpectedResult;
        if (entry.summary.temporal_periods) |periods| {
            if (!planHasExactUsizeToken(entry.plan, ":periods=", periods) and
                !planHasExactUsizeToken(entry.plan, ":add_period=", periods))
            {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_primary_key) |has_temporal_primary_key| {
            if (!planHasExactBoolToken(entry.plan, ":temporal_pk=", has_temporal_primary_key)) {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_unique) |temporal_unique| {
            if (!planHasExactUsizeToken(entry.plan, ":temporal_unique=", temporal_unique) and
                !(temporal_unique == 1 and planHasExactBoolToken(entry.plan, ":temporal_unique=", true)))
            {
                return error.TestUnexpectedResult;
            }
        }
        if (entry.summary.temporal_foreign_keys) |temporal_foreign_keys| {
            if (!planHasExactUsizeToken(entry.plan, ":temporal_fk=", temporal_foreign_keys)) {
                return error.TestUnexpectedResult;
            }
        }
    }
    if (entry.applied_plan.len > 0 and entry.family != .ddl) return error.TestUnexpectedResult;
    if (entry.applied_plan.len > 0 and !appliedPlanIsStructured(entry.applied_plan)) return error.TestUnexpectedResult;
    if (entry.apply_setup_sql.len > 0 and !corpusFixtureFamilyAllowsSetupSql(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (mode == .generated_fixture and entry.family == .ddl and entry.apply_setup_sql.len > 0 and entry.applied_plan.len == 0) {
        return error.TestUnexpectedResult;
    }
    for (entry.apply_setup_sql) |setup_sql| {
        if (setup_sql.len == 0) return error.TestUnexpectedResult;
    }
    if (entry.source_schema_json.len > 0 and !corpusFixtureFamilyAllowsSourceSchema(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.returning_rows.len > 0 and !corpusFixtureFamilyAllowsReturningRows(entry.family)) {
        return error.TestUnexpectedResult;
    }
    if (entry.returning_rows.len > 0) {
        if (entry.summary.returning == null or entry.summary.returning.? != entry.returning_rows.len) {
            return error.TestUnexpectedResult;
        }
        if (!planHasExactUsizeToken(entry.plan, ":returning_rows=", entry.returning_rows.len)) {
            return error.TestUnexpectedResult;
        }
    }
    if (!corpusFixtureSqlParameterCoverageMatches(entry)) return error.TestUnexpectedResult;
    const has_resolver_hint = entry.resolver_row_json.len > 0 or
        entry.resolver_version != 0 or
        entry.resolver_exists != null;
    if (has_resolver_hint and !corpusFixtureFamilyAllowsResolverHint(entry.family)) return error.TestUnexpectedResult;
    if (has_resolver_hint and
        (entry.family == .insert or entry.family == .unsupported_insert) and
        std.mem.indexOf(u8, entry.sql, "ON CONFLICT") == null)
    {
        return error.TestUnexpectedResult;
    }
    if (entry.resolver_row_json.len > 0 and entry.resolver_version == 0) return error.TestUnexpectedResult;
    if (entry.resolver_row_json.len == 0 and entry.resolver_version != 0) return error.TestUnexpectedResult;
    if (entry.resolver_exists == false and
        (entry.resolver_row_json.len > 0 or entry.resolver_version != 0))
    {
        return error.TestUnexpectedResult;
    }
    if (entry.resolver_exists == true and entry.resolver_row_json.len == 0) return error.TestUnexpectedResult;
    if (mode == .generated_fixture and try corpusDdlFixtureRequiresAppliedPlan(entry)) {
        if (entry.applied_plan.len == 0) return error.TestUnexpectedResult;
    }
}

pub fn validateSourceCorpusEntryMetadata(entry: AppParityCorpusEntry) !void {
    return validateCorpusMetadataCore(entry, .source);
}

pub fn validateFixtureMetadataCore(entry: AppParityCorpusEntry) !void {
    return validateCorpusMetadataCore(entry, .generated_fixture);
}

fn validateSourceCorpusEntryJsonPayloads(alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
    for (entry.returning_rows) |row_json| {
        if (!(try fixtureJsonTextIsObjectAlloc(alloc, row_json))) return error.TestUnexpectedResult;
    }
    if (entry.resolver_row_json.len > 0 and !(try fixtureJsonTextIsObjectAlloc(alloc, entry.resolver_row_json))) {
        return error.TestUnexpectedResult;
    }
}

fn fixtureJsonTextIsObjectAlloc(alloc: std.mem.Allocator, text: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, text, .{}) catch return false;
    defer parsed.deinit();
    return parsed.value == .object;
}

pub fn corpusFixtureDdlOperationsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_table,
        .relation_lifetime,
        => planUsizeOptionalTokenSumMatches(entry.plan, &.{ ":unique=", ":fk=", ":checks=" }, expected),
        .alter_table,
        .alter_domain,
        .alter_database,
        .alter_sequence,
        => planHasExactUsizeToken(entry.plan, ":ops=", expected),
        .create_sequence => planHasExactUsizeToken(entry.plan, ":options=", expected),
        .identity_allocator => (planBoolTokenUsize(entry.plan, ":primary=") orelse return false) == expected,
        .create_function,
        .create_procedure,
        .drop_function,
        .drop_procedure,
        => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .alter_role => (planNonNoneStringTokenUsize(entry.plan, ":setting=") orelse return false) == expected,
        .grant_privilege,
        .revoke_privilege,
        => planHasExactUsizeToken(entry.plan, ":privileges=", expected),
        .copy_from,
        .copy_to,
        => planHasExactUsizeToken(entry.plan, ":columns=", expected),
        .create_partitioned_table => planHasExactUsizeToken(entry.plan, ":keys=", expected),
        .create_table_partition,
        .attach_table_partition,
        .detach_table_partition,
        => expected == 0,
        .enable_row_security,
        .disable_row_security,
        .create_row_policy,
        .drop_row_policy,
        .create_update_policy,
        => expected == 1,
        .create_database,
        .drop_database,
        .create_tablespace,
        .rename_tablespace,
        .drop_tablespace,
        .listen_notification,
        .unlisten_notification,
        .alter_subscription,
        .drop_subscription,
        .drop_publication,
        .create_schema_namespace,
        .rename_schema_namespace,
        .drop_schema_namespace,
        .create_extension,
        .alter_extension_update,
        .drop_extension,
        .create_cast,
        .drop_cast,
        .deallocate_statement,
        .declare_cursor,
        .close_cursor,
        .savepoint_transaction,
        .release_savepoint,
        .rollback_to_savepoint,
        .set_search_path,
        .reset_search_path,
        .show_search_path,
        .discard_all,
        => expected == 0,
        .create_publication => planHasExactUsizeToken(entry.plan, ":tables=", expected),
        .alter_publication => planHasExactUsizeToken(entry.plan, ":add_tables=", expected),
        .create_subscription => planHasExactUsizeToken(entry.plan, ":publications=", expected),
        .notify_notification => (planBoolTokenUsize(entry.plan, ":payload=") orelse return false) == expected,
        .create_collation,
        .create_operator,
        .create_aggregate,
        => planHasExactUsizeToken(entry.plan, ":options=", expected),
        .drop_operator,
        .drop_aggregate,
        => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .vacuum_maintenance => planBoolTokenSumMatches(entry.plan, &.{ ":full=", ":freeze=", ":verbose=", ":analyze=" }, expected),
        .analyze_maintenance => (planUsizeTokenValue(entry.plan, ":columns=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":verbose=") orelse return false) == expected,
        .reindex_maintenance => (planBoolTokenUsize(entry.plan, ":concurrently=") orelse return false) == expected,
        .cluster_maintenance => (planNonNoneStringTokenUsize(entry.plan, ":index=") orelse return false) == expected,
        .prepare_statement => planHasExactUsizeToken(entry.plan, ":params=", expected),
        .execute_statement => planHasExactUsizeToken(entry.plan, ":args=", expected),
        .comment_metadata => (planBoolTokenUsize(entry.plan, ":comment=") orelse return false) == expected,
        .table_lock => planHasExactUsizeToken(entry.plan, ":tables=", expected),
        .constraint_mode => (planUsizeTokenValue(entry.plan, ":constraints=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":all=") orelse return false) == expected,
        .transaction_mode => planNonNoneStringTokenSumMatches(entry.plan, &.{ ":isolation=", ":access=", ":deferrable=" }, expected),
        .advisory_lock => planHasExactUsizeToken(entry.plan, ":keys=", expected),
        .fetch_cursor => planHasExactUsizeToken(entry.plan, ":count=", expected) or expected == 0,
        .create_index,
        .drop_index,
        .drop_table,
        .create_view,
        .rename_view,
        .drop_view,
        .create_materialized_view,
        .refresh_materialized_view,
        .drop_materialized_view,
        .table_clone,
        .create_enum_type,
        .add_enum_value,
        .drop_enum_type,
        .create_domain,
        .drop_domain,
        .drop_sequence,
        .create_role,
        .drop_role,
        .rename_collation,
        .drop_collation,
        => false,
    };
}

pub fn corpusFixtureOperationsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .ddl => corpusFixtureDdlOperationsSummaryMatchesPlan(entry, expected),
        .insert,
        .update,
        .update_source,
        .update_joined_source,
        => planHasExactUsizeToken(entry.plan, ":ops=", expected),
        .insert_source => planHasExactUsizeToken(entry.plan, ":assignments=", expected),
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":matched_update=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsReturningSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert,
        .insert_source,
        .update,
        .delete,
        .update_source,
        .delete_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        .explain => explainPlanHasKind(entry.plan, "write"),
        else => false,
    };
}

pub fn corpusFixtureReturningSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .insert,
        .update,
        .delete,
        => planHasExactUsizeToken(entry.plan, ":returning_rows=", expected),
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => planHasExactUsizeToken(entry.plan, ":returning=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":returning_rows=", expected) or
            planHasExactUsizeToken(entry.plan, ":returning=", expected),
        else => false,
    };
}

pub fn corpusFixtureReturningAllSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: bool) bool {
    return corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":returning_all=", expected);
}

pub fn corpusFixtureAllowsConflictWhereSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert,
        .insert_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"),
        else => false,
    };
}

pub fn corpusFixtureConflictWhereSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: bool) bool {
    return corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":conflict_where=", expected);
}

pub fn corpusFixtureAllowsMutationTransformSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert_source,
        .update_source,
        .update_joined_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureAllowsSourceAssignmentsSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .update_joined_source => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureSourceAssignmentsSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .update_joined_source,
        .explain,
        => planHasExactUsizeToken(entry.plan, ":source_assignments=", expected),
        else => false,
    };
}

pub fn corpusFixtureTransformSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.patch_expressions) |patch_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_patch_expr="
        else
            ":patch_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, patch_expressions)) return false;
    }
    if (entry.summary.increment_expressions) |increment_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_increment_expr="
        else
            ":increment_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, increment_expressions)) return false;
    }
    if (entry.summary.json_set_expressions) |json_set_expressions| {
        const token = if (entry.family == .insert_source or corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:"))
            ":conflict_json_set_expr="
        else
            ":json_set_expr=";
        if (!planHasExactUsizeToken(entry.plan, token, json_set_expressions)) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsMergeArmSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .merge_mutation => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:"),
        else => false,
    };
}

pub fn corpusFixtureMergeArmSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.matched_predicates) |matched_predicates| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_pred=", matched_predicates)) return false;
    }
    if (entry.summary.matched_delete) |matched_delete| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_delete=", @intFromBool(matched_delete))) return false;
    }
    if (entry.summary.matched_do_nothing) |matched_do_nothing| {
        if (!planHasExactUsizeToken(entry.plan, ":matched_noop=", @intFromBool(matched_do_nothing))) return false;
    }
    if (entry.summary.not_matched_predicates) |not_matched_predicates| {
        if (!planHasExactUsizeToken(entry.plan, ":not_matched_pred=", not_matched_predicates)) return false;
    }
    if (entry.summary.not_matched_do_nothing) |not_matched_do_nothing| {
        if (!planHasExactUsizeToken(entry.plan, ":not_matched_noop=", @intFromBool(not_matched_do_nothing))) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsAggregateSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .aggregate => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:aggregate:"),
        else => false,
    };
}

pub fn corpusFixtureAggregateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.group_by) |group_by| {
        if (!planHasExactUsizeToken(entry.plan, ":group=", group_by)) return false;
    }
    if (entry.summary.group_expressions) |group_expressions| {
        if (!planHasExactUsizeToken(entry.plan, ":group_expr=", group_expressions)) return false;
    }
    if (entry.summary.aggregations) |aggregations| {
        if (!planHasExactUsizeToken(entry.plan, ":aggs=", aggregations)) return false;
    }
    if (entry.summary.filter_groups) |filter_groups| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":filter_groups=", filter_groups)) return false;
    }
    if (entry.summary.having) |having| {
        if (!planHasExactUsizeToken(entry.plan, ":having=", having)) return false;
    }
    if (entry.summary.having_expressions) |having_expressions| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_expr=", having_expressions)) return false;
    }
    if (entry.summary.having_any) |having_any| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_any=", having_any)) return false;
    }
    if (entry.summary.having_not) |having_not| {
        if (!corpusOptionalZeroSummaryMatchesPlan(entry.plan, ":having_not=", having_not)) return false;
    }
    return true;
}

pub fn corpusFixtureDdlSelectSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.select orelse return true;
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_table,
        .relation_lifetime,
        .create_partitioned_table,
        => planHasExactUsizeToken(entry.plan, ":columns=", expected),
        .identity_allocator => (planUsizeTokenValue(entry.plan, ":columns=") orelse return false) +
            (planBoolTokenUsize(entry.plan, ":primary=") orelse return false) == expected,
        .create_index => planUsizeOptionalTokenSumMatches(entry.plan, &.{ ":columns=", ":expr=", ":generated_expr=" }, expected),
        .create_view,
        .create_materialized_view,
        => planHasExactUsizeToken(entry.plan, ":fields=", expected),
        .create_enum_type => planHasExactUsizeToken(entry.plan, ":values=", expected),
        .create_aggregate => planHasExactUsizeToken(entry.plan, ":args=", expected),
        else => false,
    };
}

pub fn corpusFixtureDdlPredicateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.predicates orelse return true;
    if (entry.family != .ddl) return true;
    return switch (entry.summary.ddl_tag orelse return false) {
        .create_index => planHasExactUsizeToken(entry.plan, ":where=", expected),
        .create_domain => planHasExactUsizeToken(entry.plan, ":checks=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsPredicateSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .ddl,
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureSidePredicateSummaryMatchesPlan(plan: []const u8, expected: usize) bool {
    return planUsizeTokenSumMatches(plan, &.{ ":left_pred=", ":right_pred=" }, expected);
}

pub fn corpusFixturePredicateSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.predicates orelse return true;
    return switch (entry.family) {
        .ddl => true,
        .query => planHasExactUsizeToken(entry.plan, ":pred=", expected),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, ":source_pred=", expected),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, ":pred=", expected)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, ":source_pred=", expected)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, ":pred=", expected) or
            planHasExactUsizeToken(entry.plan, ":source_pred=", expected) or
            corpusFixtureSidePredicateSummaryMatchesPlan(entry.plan, expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsAccessSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureSideAccessSummaryMatchesPlan(
    plan: []const u8,
    left_token: []const u8,
    right_token: []const u8,
    expected: usize,
) bool {
    return planUsizeOptionalTokenSumMatches(plan, &.{ left_token, right_token }, expected);
}

fn corpusFixtureSideTextPatternSummaryMatchesPlan(plan: []const u8, expected: usize) bool {
    return planUsizeOptionalTokenSumMatches(plan, &.{ ":left_text=", ":right_text=" }, expected) or
        planUsizeOptionalTokenSumMatches(plan, &.{ ":left_text_pattern=", ":right_text_pattern=" }, expected);
}

fn corpusFixtureAccessSummaryFieldMatchesPlan(
    entry: AppParityCorpusEntry,
    expected: ?usize,
    row_token: []const u8,
    source_token: []const u8,
    left_token: []const u8,
    right_token: []const u8,
) bool {
    const value = expected orelse return true;
    return switch (entry.family) {
        .query => planHasExactUsizeToken(entry.plan, row_token, value),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, source_token, value),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, row_token, value)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, source_token, value)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, row_token, value) or
            planHasExactUsizeToken(entry.plan, source_token, value) or
            corpusFixtureSideAccessSummaryMatchesPlan(entry.plan, left_token, right_token, value),
        else => false,
    };
}

fn corpusFixtureTextPatternSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const value = entry.summary.text_patterns orelse return true;
    return switch (entry.family) {
        .query => planHasExactUsizeToken(entry.plan, ":text_pattern=", value),
        .aggregate,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value),
        .join,
        .lateral,
        .update_joined_source,
        .delete_joined_source,
        => corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value),
        .read => if (corpusReadPlanHasPrefix(entry, "read:query:"))
            planHasExactUsizeToken(entry.plan, ":text_pattern=", value)
        else if (corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:window:"))
            planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value)
        else if (corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"))
            corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value)
        else
            false,
        .explain => planHasExactUsizeToken(entry.plan, ":text_pattern=", value) or
            planHasExactUsizeToken(entry.plan, ":source_text_pattern=", value) or
            corpusFixtureSideTextPatternSummaryMatchesPlan(entry.plan, value),
        else => false,
    };
}

pub fn corpusFixtureAccessSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const summary = entry.summary;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_any, ":array_any=", ":source_array_any=", ":left_array_any=", ":right_array_any=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.in_predicates, ":in=", ":source_in=", ":left_in=", ":right_in=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_path_eq, ":json_eq=", ":source_json_eq=", ":left_json_eq=", ":right_json_eq=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_contains, ":json_contains=", ":source_json_contains=", ":left_json_contains=", ":right_json_contains=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.json_path_exists, ":json_exists=", ":source_json_exists=", ":left_json_exists=", ":right_json_exists=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_contains, ":array_contains=", ":source_array_contains=", ":left_array_contains=", ":right_array_contains=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.array_eq, ":array_eq=", ":source_array_eq=", ":left_array_eq=", ":right_array_eq=")) return false;
    if (!corpusFixtureTextPatternSummaryMatchesPlan(entry)) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.access_or_predicates, ":access_or=", ":source_access_or=", ":left_access_or=", ":right_access_or=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.access_not_predicates, ":access_not=", ":source_access_not=", ":left_access_not=", ":right_access_not=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_predicates, ":expr_pred=", ":source_expr_pred=", ":left_expr_pred=", ":right_expr_pred=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_or_predicates, ":expr_or=", ":source_expr_or=", ":left_expr_or=", ":right_expr_or=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_not_predicates, ":expr_not=", ":source_expr_not=", ":left_expr_not=", ":right_expr_not=")) return false;
    if (!corpusFixtureAccessSummaryFieldMatchesPlan(entry, summary.expression_array_contains, ":expr_array=", ":source_expr_array=", ":left_expr_array=", ":right_expr_array=")) return false;
    return true;
}

pub fn corpusFixtureSelectSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const expected = entry.summary.select orelse return true;
    return switch (entry.family) {
        .ddl => true,
        .query,
        .read,
        .window,
        => planHasExactUsizeToken(entry.plan, ":select=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":select=", expected) or
            planHasExactUsizeToken(entry.plan, ":not_matched_insert=", expected),
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":not_matched_insert=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsJoinSelectSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .join,
        .lateral,
        => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"),
        else => false,
    };
}

pub fn corpusFixtureJoinSelectSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return planHasExactUsizeToken(entry.plan, ":select=", expected);
}

pub fn corpusFixtureAllowsJoinOnSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .join,
        .update_joined_source,
        .delete_joined_source,
        .merge_mutation,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:join:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=merge_mutation:"),
        else => false,
    };
}

pub fn corpusFixtureJoinOnSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return switch (entry.family) {
        .merge_mutation => planHasExactUsizeToken(entry.plan, ":match=", expected),
        .join,
        .update_joined_source,
        .delete_joined_source,
        .read,
        => planHasExactUsizeToken(entry.plan, ":on=", expected),
        .explain => planHasExactUsizeToken(entry.plan, ":on=", expected) or
            planHasExactUsizeToken(entry.plan, ":match=", expected),
        else => false,
    };
}

pub fn corpusFixtureAllowsLateralSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .lateral => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:lateral:"),
        else => false,
    };
}

pub fn corpusFixtureLateralSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.lateral_correlations) |correlations| {
        if (!planHasExactUsizeToken(entry.plan, ":corr=", correlations)) return false;
    }
    if (entry.summary.right_offset) |right_offset| {
        if (!planHasExactUsizeToken(entry.plan, ":right_offset=", @intCast(right_offset))) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsWindowSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .window => true,
        .read, .explain => corpusReadPlanHasPrefix(entry, "read:window:"),
        else => false,
    };
}

pub fn corpusFixtureWindowSummaryMatchesPlan(entry: AppParityCorpusEntry, expected: usize) bool {
    return planHasExactUsizeToken(entry.plan, ":windows=", expected);
}

pub fn corpusFixtureAllowsFullQueryOutputSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:"),
        else => false,
    };
}

pub fn corpusFixtureFullQueryOutputSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    if (entry.summary.select_all) |select_all| {
        if (!corpusOptionalBool01SummaryMatchesPlan(entry.plan, ":select_all=", select_all)) return false;
    }
    if (entry.summary.distinct_on) |distinct_on| {
        if (!planHasExactUsizeToken(entry.plan, ":distinct_on=", distinct_on)) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsPaginationSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .aggregate,
        .join,
        .lateral,
        .window,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:aggregate:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusReadPlanHasPrefix(entry, "read:window:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

fn corpusFixtureUsesSourcePagination(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        => true,
        .explain => corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:"),
        else => false,
    };
}

pub fn corpusFixturePaginationSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const source_pagination = corpusFixtureUsesSourcePagination(entry);
    if (entry.summary.order_by) |order_by| {
        const token_text = if (source_pagination) ":source_order=" else ":order=";
        if (!planHasExactUsizeToken(entry.plan, token_text, order_by)) return false;
    }
    if (entry.summary.limit) |limit| {
        const token_text = if (source_pagination) ":source_limit=" else ":limit=";
        if (!planHasExactUsizeToken(entry.plan, token_text, limit)) return false;
    }
    if (entry.summary.offset) |offset| {
        const token_text = if (source_pagination) ":source_offset=" else ":offset=";
        if (!planHasExactUsizeToken(entry.plan, token_text, offset)) return false;
    }
    return true;
}

pub fn corpusFixtureAllowsRowClaimSummary(entry: AppParityCorpusEntry) bool {
    return switch (entry.family) {
        .query,
        .join,
        .lateral,
        .insert_source,
        .update_source,
        .delete_source,
        .truncate_source,
        .update_joined_source,
        .delete_joined_source,
        => true,
        .read => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:"),
        .explain => corpusReadPlanHasPrefix(entry, "read:query:") or
            corpusReadPlanHasPrefix(entry, "read:join:") or
            corpusReadPlanHasPrefix(entry, "read:lateral:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=insert_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=truncate_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=update_joined_source:") or
            corpusExplainWriteInnerHasPrefix(entry, ":inner=delete_joined_source:"),
        else => false,
    };
}

pub fn corpusFixtureRowClaimSummaryMatchesPlan(entry: AppParityCorpusEntry) bool {
    const skip_locked = entry.summary.row_claim_skip_locked orelse return true;
    if (skip_locked) {
        return planHasExactStringToken(entry.plan, ":claim=", "skip_locked") or
            planHasExactStringToken(entry.plan, ":claim=", "no_key_update_skip_locked");
    }
    return planHasExactStringToken(entry.plan, ":claim=", "locked") or
        planHasExactStringToken(entry.plan, ":claim=", "nowait") or
        planHasExactStringToken(entry.plan, ":claim=", "no_key_update") or
        planHasExactStringToken(entry.plan, ":claim=", "no_key_update_nowait");
}

pub fn unsupportedPlanFamilyToken(family: UnsupportedPlanFamily) []const u8 {
    return @tagName(family);
}

pub fn unsupportedFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "unsupported:{s}:requires={s}", .{
        unsupportedPlanFamilyToken(family),
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "adapter_noop:{s}:reason={s}", .{
        family,
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn boolFingerprintValue(value: bool) u8 {
    return if (value) 1 else 0;
}

pub fn appendNonZeroU32FingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: u32,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendNonZeroUsizeFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: usize,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendNamedNonZeroUsizeFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    prefix: []const u8,
    label: []const u8,
    value: usize,
) ![]u8 {
    if (value == 0) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}_{s}={d}", .{ owned_base, prefix, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn appendTrueBoolFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: bool,
) ![]u8 {
    if (!value) return owned_base;
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}=1", .{ owned_base, label });
    alloc.free(owned_base);
    return out;
}

pub fn appendBoolFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: bool,
) ![]u8 {
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={d}", .{ owned_base, label, boolFingerprintValue(value) });
    alloc.free(owned_base);
    return out;
}

pub fn appendStringFingerprintAlloc(
    alloc: std.mem.Allocator,
    owned_base: []u8,
    label: []const u8,
    value: []const u8,
) ![]u8 {
    errdefer alloc.free(owned_base);
    const out = try std.fmt.allocPrint(alloc, "{s}:{s}={s}", .{ owned_base, label, value });
    alloc.free(owned_base);
    return out;
}

pub fn unsupportedPlanMatchesFamily(plan: []const u8, family: UnsupportedPlanFamily) bool {
    const prefix = "unsupported:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const family_token = unsupportedPlanFamilyToken(family);
    const rest = plan[prefix.len..];
    return std.mem.startsWith(u8, rest, family_token) and
        rest.len > family_token.len and
        rest[family_token.len] == ':';
}

pub fn unsupportedPlanMatchesReason(
    plan: []const u8,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return false;
    if (!unsupportedPlanMatchesFamily(plan, family)) return false;
    return planHasExactStringToken(plan, ":requires=", diagnostics.classificationReasonToken(reason));
}

pub fn adapterNoopPlanMatchesReason(
    plan: []const u8,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return false;
    const prefix = "adapter_noop:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const rest = plan[prefix.len..];
    if (!std.mem.startsWith(u8, rest, family) or rest.len <= family.len or rest[family.len] != ':') return false;
    return planHasExactStringToken(plan, ":reason=", diagnostics.classificationReasonToken(reason));
}

pub const PlanStringTokenScan = union(enum) {
    absent,
    value: []const u8,
    invalid,
};

pub fn scanStringToken(plan: []const u8, token: []const u8) PlanStringTokenScan {
    var start: usize = 0;
    var found: ?[]const u8 = null;
    while (std.mem.indexOfPos(u8, plan, start, token)) |index| {
        const value_start = index + token.len;
        var value_end = value_start;
        while (value_end < plan.len and plan[value_end] != ':') : (value_end += 1) {}
        if (found != null) return .invalid;
        found = plan[value_start..value_end];
        start = index + token.len;
    }
    if (found) |value| return .{ .value = value };
    return .absent;
}

pub fn planHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| std.mem.eql(u8, value, expected),
        .absent, .invalid => false,
    };
}

pub fn planHasStringToken(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| value.len > 0,
        .absent, .invalid => false,
    };
}

pub fn planTokenAbsent(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .absent => true,
        .value, .invalid => false,
    };
}

pub fn planHasAnyExactStringToken(plan: []const u8, token: []const u8, expected_values: []const []const u8) bool {
    for (expected_values) |expected| {
        if (planHasExactStringToken(plan, token, expected)) return true;
    }
    return false;
}

pub fn parseDelimitedUsizeToken(plan: []const u8, value_start: usize) ?usize {
    var pos = value_start;
    if (pos >= plan.len or plan[pos] < '0' or plan[pos] > '9') return null;
    var value: usize = 0;
    while (pos < plan.len and plan[pos] >= '0' and plan[pos] <= '9') : (pos += 1) {
        value = value * 10 + @as(usize, plan[pos] - '0');
    }
    if (pos != plan.len and plan[pos] != ':') return null;
    return value;
}

pub const PlanUsizeTokenScan = union(enum) {
    absent,
    value: usize,
    invalid,
};

pub fn scanUsizeToken(plan: []const u8, token: []const u8) PlanUsizeTokenScan {
    var start: usize = 0;
    var found: ?usize = null;
    while (std.mem.indexOfPos(u8, plan, start, token)) |index| {
        const parsed = parseDelimitedUsizeToken(plan, index + token.len) orelse return .invalid;
        if (found != null) return .invalid;
        found = parsed;
        start = index + token.len;
    }
    if (found) |value| return .{ .value = value };
    return .absent;
}

pub fn planUsizeTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent, .invalid => null,
    };
}

pub fn planHasNonZeroToken(plan: []const u8, token: []const u8) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value > 0,
        .absent, .invalid => false,
    };
}

pub fn planHasNonZeroUsizeTokenNamePrefix(plan: []const u8, name_prefix: []const u8) bool {
    var segment_start: usize = 0;
    var found_non_zero = false;
    while (segment_start < plan.len) {
        var segment_end = segment_start;
        while (segment_end < plan.len and plan[segment_end] != ':') : (segment_end += 1) {}
        const segment = plan[segment_start..segment_end];
        if (std.mem.indexOfScalar(u8, segment, '=')) |equals_index| {
            if (std.mem.startsWith(u8, segment[0..equals_index], name_prefix)) {
                const value = parseDelimitedUsizeToken(segment, equals_index + 1) orelse return false;
                found_non_zero = found_non_zero or value > 0;
            }
        }
        segment_start = segment_end + 1;
    }
    return found_non_zero;
}

pub fn planHasExactUsizeToken(plan: []const u8, token: []const u8, expected: usize) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value == expected,
        .absent, .invalid => false,
    };
}

pub fn planUsizeOptionalTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent => 0,
        .invalid => null,
    };
}

pub fn planBoolTokenValue(plan: []const u8, token: []const u8) ?bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk true;
            if (std.mem.eql(u8, value, "false")) break :blk false;
            break :blk null;
        },
        .absent, .invalid => null,
    };
}

pub fn planBoolTokenUsize(plan: []const u8, token: []const u8) ?usize {
    return switch (scanStringToken(plan, token)) {
        .absent => 0,
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk 1;
            if (std.mem.eql(u8, value, "false")) break :blk 0;
            break :blk null;
        },
        .invalid => null,
    };
}

pub fn planHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    const value = planBoolTokenValue(plan, token) orelse return false;
    return value == expected;
}

pub fn planUsizeTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        const value = planUsizeTokenValue(plan, token) orelse return false;
        sum += value;
    }
    return sum == expected;
}

pub fn planUsizeOptionalTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planUsizeOptionalTokenValue(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planNonNoneStringTokenUsize(plan: []const u8, token: []const u8) ?usize {
    return switch (scanStringToken(plan, token)) {
        .absent => 0,
        .value => |value| if (std.mem.eql(u8, value, "none")) 0 else 1,
        .invalid => null,
    };
}

pub fn planNonNoneStringTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planNonNoneStringTokenUsize(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planBoolTokenSumMatches(plan: []const u8, tokens: []const []const u8, expected: usize) bool {
    var sum: usize = 0;
    for (tokens) |token| {
        sum += planBoolTokenUsize(plan, token) orelse return false;
    }
    return sum == expected;
}

pub fn planHasAnyNonZeroToken(plan: []const u8, tokens: []const []const u8) bool {
    for (tokens) |token| {
        if (planHasNonZeroToken(plan, token)) return true;
    }
    return false;
}

pub fn explainPlanHasKind(plan: []const u8, expected: []const u8) bool {
    return planHasExactStringToken(plan, "explain:kind=", expected);
}

pub fn explainPlanInnerStartsWith(plan: []const u8, inner_prefix: []const u8) bool {
    const inner_token = ":inner=";
    const inner_index = std.mem.indexOf(u8, plan, inner_token) orelse return false;
    if (std.mem.indexOfPos(u8, plan, inner_index + inner_token.len, inner_token) != null) return false;
    return std.mem.startsWith(u8, plan[inner_index + inner_token.len ..], inner_prefix);
}

pub fn joinedSourcePlanHasCounts(plan: []const u8, right_predicates: usize, join_keys: usize) bool {
    return planHasExactUsizeToken(plan, ":right_pred=", right_predicates) and
        planHasExactUsizeToken(plan, ":on=", join_keys);
}

pub fn writePlanHasCounts(plan: []const u8, writes: usize, transforms: usize) bool {
    return planHasExactUsizeToken(plan, ":writes=", writes) and
        planHasExactUsizeToken(plan, ":transforms=", transforms);
}

pub fn appliedPlanIsStructured(plan: []const u8) bool {
    if (std.mem.startsWith(u8, plan, "applied:drop_table:")) {
        var drop_index: usize = 0;
        if (!consumeLiteral(plan, &drop_index, "applied:drop_table:rebuild=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        if (!consumeLiteral(plan, &drop_index, ":validation=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        if (!consumeLiteral(plan, &drop_index, ":rewrite=")) return false;
        if (!consumeBool(plan, &drop_index)) return false;
        return drop_index == plan.len;
    }

    var index: usize = 0;
    if (!consumeLiteral(plan, &index, "applied:rebuild=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":validation=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":rewrite=")) return false;
    if (!consumeBool(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":building_indexes=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_unique=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_fk=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":unvalidated_check=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (!consumeLiteral(plan, &index, ":update_policy=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    if (index == plan.len) return true;
    if (!consumeLiteral(plan, &index, ":comments=")) return false;
    if (!consumeUsize(plan, &index)) return false;
    return index == plan.len;
}

pub fn appliedPlanHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    return appliedPlanIsStructured(plan) and planHasExactBoolToken(plan, token, expected);
}

pub fn appliedPlanHasExactUsizeToken(plan: []const u8, token: []const u8, expected: usize) bool {
    return appliedPlanIsStructured(plan) and planHasExactUsizeToken(plan, token, expected);
}

fn consumeLiteral(text: []const u8, index: *usize, literal: []const u8) bool {
    if (index.* > text.len) return false;
    if (text.len - index.* < literal.len) return false;
    if (!std.mem.eql(u8, text[index.* .. index.* + literal.len], literal)) return false;
    index.* += literal.len;
    return true;
}

fn consumeBool(text: []const u8, index: *usize) bool {
    if (consumeLiteral(text, index, "true")) return true;
    if (consumeLiteral(text, index, "false")) return true;
    return false;
}

fn consumeUsize(text: []const u8, index: *usize) bool {
    const start = index.*;
    while (index.* < text.len and text[index.*] >= '0' and text[index.*] <= '9') : (index.* += 1) {}
    return index.* > start;
}

pub const SqlParameterScan = union(enum) {
    absent,
    value: usize,
    invalid,
};

pub fn sqlHasParameterIndex(sql: []const u8, expected: usize) bool {
    var index: usize = 0;
    while (sqlNextParameter(sql, &index)) |scan| {
        switch (scan) {
            .value => |param_index| if (param_index == expected) return true,
            .absent, .invalid => {},
        }
    }
    return false;
}

pub fn sqlParameterCoverageMatches(sql: []const u8, param_count: usize) bool {
    var index: usize = 0;
    var saw_parameter = false;
    var max_index: usize = 0;
    while (sqlNextParameter(sql, &index)) |scan| {
        switch (scan) {
            .value => |param_index| {
                if (param_index == 0 or param_index > param_count) return false;
                saw_parameter = true;
                max_index = @max(max_index, param_index);
            },
            .invalid => return false,
            .absent => {},
        }
    }
    if (param_count == 0) return !saw_parameter;
    if (!saw_parameter or max_index != param_count) return false;

    for (1..param_count + 1) |param_index| {
        if (!sqlHasParameterIndex(sql, param_index)) return false;
    }
    return true;
}

pub fn sqlNextParameter(sql: []const u8, index: *usize) ?SqlParameterScan {
    while (index.* < sql.len) {
        switch (sql[index.*]) {
            '\'' => {
                index.* = sqlSingleQuotedEnd(sql, index.*);
                continue;
            },
            '"' => {
                index.* = sqlDoubleQuotedEnd(sql, index.*);
                continue;
            },
            '-' => {
                if (index.* + 1 < sql.len and sql[index.* + 1] == '-') {
                    index.* = sqlLineCommentEnd(sql, index.*);
                    continue;
                }
            },
            '/' => {
                if (index.* + 1 < sql.len and sql[index.* + 1] == '*') {
                    index.* = sqlBlockCommentEnd(sql, index.*);
                    continue;
                }
            },
            '$' => {
                const dollar = index.*;
                if (sqlParameterIndexAt(sql, dollar)) |scan| {
                    index.* = sqlParameterTokenEnd(sql, dollar);
                    return scan;
                }
                if (sqlDollarQuotedEnd(sql, dollar)) |end| {
                    index.* = end;
                    continue;
                }
            },
            else => {},
        }
        index.* += 1;
    }
    return null;
}

fn sqlParameterIndexAt(sql: []const u8, dollar: usize) ?SqlParameterScan {
    if (dollar + 1 >= sql.len) return null;
    if (sql[dollar] != '$') return null;
    if (sql[dollar + 1] < '0' or sql[dollar + 1] > '9') return null;

    var index = dollar + 1;
    var value: usize = 0;
    while (index < sql.len and sql[index] >= '0' and sql[index] <= '9') : (index += 1) {
        value = value * 10 + (sql[index] - '0');
    }
    if (index < sql.len and (std.ascii.isAlphanumeric(sql[index]) or sql[index] == '_')) return .invalid;
    return .{ .value = value };
}

fn sqlParameterTokenEnd(sql: []const u8, dollar: usize) usize {
    var index = dollar + 1;
    while (index < sql.len and sql[index] >= '0' and sql[index] <= '9') : (index += 1) {}
    return index;
}

fn sqlSingleQuotedEnd(sql: []const u8, quote: usize) usize {
    var index = quote + 1;
    while (index < sql.len) : (index += 1) {
        if (sql[index] != '\'') continue;
        if (index + 1 < sql.len and sql[index + 1] == '\'') {
            index += 1;
            continue;
        }
        return index + 1;
    }
    return sql.len;
}

fn sqlDoubleQuotedEnd(sql: []const u8, quote: usize) usize {
    var index = quote + 1;
    while (index < sql.len) : (index += 1) {
        if (sql[index] != '"') continue;
        if (index + 1 < sql.len and sql[index + 1] == '"') {
            index += 1;
            continue;
        }
        return index + 1;
    }
    return sql.len;
}

fn sqlLineCommentEnd(sql: []const u8, dash: usize) usize {
    var index = dash + 2;
    while (index < sql.len and sql[index] != '\n' and sql[index] != '\r') : (index += 1) {}
    return index;
}

fn sqlBlockCommentEnd(sql: []const u8, slash: usize) usize {
    var index = slash + 2;
    while (index + 1 < sql.len) : (index += 1) {
        if (sql[index] == '*' and sql[index + 1] == '/') return index + 2;
    }
    return sql.len;
}

fn sqlDollarQuotedEnd(sql: []const u8, dollar: usize) ?usize {
    if (dollar + 1 >= sql.len) return null;
    if (sql[dollar + 1] >= '0' and sql[dollar + 1] <= '9') return null;

    var delimiter_end = dollar + 1;
    if (sql[delimiter_end] == '$') {
        delimiter_end += 1;
    } else {
        if (!sqlDollarQuoteTagStart(sql[delimiter_end])) return null;
        delimiter_end += 1;
        while (delimiter_end < sql.len and sqlDollarQuoteTagContinue(sql[delimiter_end])) : (delimiter_end += 1) {}
        if (delimiter_end >= sql.len or sql[delimiter_end] != '$') return null;
        delimiter_end += 1;
    }

    const delimiter = sql[dollar..delimiter_end];
    const body_start = delimiter_end;
    if (std.mem.indexOfPos(u8, sql, body_start, delimiter)) |close| {
        return close + delimiter.len;
    }
    return sql.len;
}

fn sqlDollarQuoteTagStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_';
}

fn sqlDollarQuoteTagContinue(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

test "sql adapter corpus parses fixture entries and owns allocated slices" {
    const alloc = std.testing.allocator;
    const fixture_entry_json =
        \\{
        \\  "name": "joined update",
        \\  "family": "update_joined_source",
        \\  "summary": {
        \\    "table_name": "usage_records",
        \\    "patch_expressions": 2
        \\  },
        \\  "plan": "update_joined_source:table=usage_records:source_assignments=2",
        \\  "classification_reason": "",
        \\  "apply_setup_sql": ["CREATE TABLE usage_records (id text PRIMARY KEY)"],
        \\  "returning_rows": ["{\"id\":\"u1\"}"],
        \\  "applied_plan": "",
        \\  "resolver_row_json": "",
        \\  "resolver_version": 9,
        \\  "resolver_exists": true,
        \\  "source_schema_json": "",
        \\  "params": [
        \\    {"integer": 42},
        \\    {"string": "queued"},
        \\    {"float": 2.5},
        \\    {"json": "{\"enabled\":true}"},
        \\    {"null": true}
        \\  ],
        \\  "sql": "UPDATE usage_records SET quantity = $1"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_entry_json, .{});
    defer parsed.deinit();

    const entry = try parseFixtureEntryAlloc(alloc, parsed.value);
    defer freeFixtureEntry(alloc, entry);
    try std.testing.expectEqualStrings("joined update", entry.name);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.update_joined_source, entry.family);
    try std.testing.expectEqual(@as(?usize, 2), entry.summary.source_assignments);
    try std.testing.expectEqual(@as(?usize, null), entry.summary.patch_expressions);
    try std.testing.expectEqual(@as(usize, 5), entry.params.len);
    try std.testing.expectEqual(@as(i64, 42), entry.params[0].integer);
    try std.testing.expectEqualStrings("queued", entry.params[1].string);
    try std.testing.expectEqual(@as(usize, 1), entry.apply_setup_sql.len);
    try std.testing.expectEqual(@as(usize, 1), entry.returning_rows.len);
    try std.testing.expectEqual(@as(u64, 9), entry.resolver_version);
    try std.testing.expectEqual(@as(?bool, true), entry.resolver_exists);
}

test "sql adapter corpus parses fixture root metadata and owns skipped list" {
    const alloc = std.testing.allocator;
    const fixture_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 2,
        \\  "entry_count": 1,
        \\  "skipped_entries": ["unsupported recursive cte"],
        \\  "schema_json": "{\"version\":1}",
        \\  "entries": [
        \\    {
        \\      "name": "read",
        \\      "family": "read",
        \\      "plan": "read:table=usage_records",
        \\      "sql": "SELECT * FROM usage_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();

    const root = try parseFixtureRootAlloc(alloc, parsed.value);
    defer freeFixtureRoot(alloc, root);
    try std.testing.expectEqual(app_parity_fixture_format, root.fixture_format);
    try std.testing.expectEqual(@as(usize, 2), root.source_entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.skipped_entries.len);
    try std.testing.expectEqualStrings("unsupported recursive cte", root.skipped_entries[0]);
    try std.testing.expectEqualStrings("{\"version\":1}", root.schema_json);
    try std.testing.expectEqual(@as(usize, 1), root.entries.len);
}

test "sql adapter corpus parses source corpus root entries" {
    const alloc = std.testing.allocator;
    const source_json = @embedFile("../fixtures/sql_api_parity_source_corpus.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
    defer parsed.deinit();

    const root = try parseSourceCorpusRootAlloc(alloc, parsed.value);
    defer freeSourceCorpusRoot(alloc, root);

    try std.testing.expectEqual(app_parity_source_corpus_format, root.source_format);
    try std.testing.expect(root.entries.len > 0);
    try std.testing.expectEqualStrings("prepare statement protocol plan", root.entries[0].name);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.ddl, root.entries[0].family);
    try std.testing.expectEqual(AppParityDdlTag.prepare_statement, root.entries[0].summary.ddl_tag.?);
}

test "sql adapter source corpus rejects duplicate entry names" {
    const alloc = std.testing.allocator;
    const source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "duplicate source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "show_search_path"},
        \\      "plan": "ddl:session:show_search_path",
        \\      "sql": "SHOW search_path"
        \\    },
        \\    {
        \\      "name": "duplicate source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "discard_all"},
        \\      "plan": "ddl:session:discard_all",
        \\      "sql": "DISCARD ALL"
        \\    }
        \\  ]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed.value));
}

test "sql adapter source corpus validates entry metadata while allowing derived applied plans" {
    const alloc = std.testing.allocator;
    const valid_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "comment metadata source entry",
        \\      "family": "ddl",
        \\      "summary": {"ddl_tag": "comment_metadata", "table_name": "usage_records"},
        \\      "plan": "ddl:comment:on=table:table=usage_records",
        \\      "apply_setup_sql": ["CREATE TABLE usage_records (id text PRIMARY KEY)"],
        \\      "sql": "COMMENT ON TABLE usage_records IS 'runtime records'"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_valid = try std.json.parseFromSlice(std.json.Value, alloc, valid_source_json, .{});
    defer parsed_valid.deinit();
    const valid_root = try parseSourceCorpusRootAlloc(alloc, parsed_valid.value);
    defer freeSourceCorpusRoot(alloc, valid_root);
    try std.testing.expectEqual(@as(usize, 1), valid_root.entries.len);

    const invalid_source_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "bad family plan",
        \\      "family": "query",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "insert:table=usage_records",
        \\      "sql": "SELECT id FROM usage_records"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_invalid = try std.json.parseFromSlice(std.json.Value, alloc, invalid_source_json, .{});
    defer parsed_invalid.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_invalid.value));
}

test "sql adapter source corpus validates deterministic json payloads" {
    const alloc = std.testing.allocator;
    const invalid_returning_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "array returning row",
        \\      "family": "insert",
        \\      "summary": {"table_name": "usage_records", "returning": 1},
        \\      "plan": "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
        \\      "returning_rows": ["[\"u1\"]"],
        \\      "sql": "INSERT INTO usage_records (id) VALUES ('u1') RETURNING id"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_returning = try std.json.parseFromSlice(std.json.Value, alloc, invalid_returning_json, .{});
    defer parsed_returning.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_returning.value));

    const invalid_resolver_json =
        \\{
        \\  "source_format": 1,
        \\  "entries": [
        \\    {
        \\      "name": "array resolver row",
        \\      "family": "insert",
        \\      "summary": {"table_name": "usage_records"},
        \\      "plan": "insert:table=usage_records:writes=0:transforms=1:ops=1:deletes=0:returning_rows=0:returning_expr=0:op_set=1",
        \\      "resolver_row_json": "[\"u1\"]",
        \\      "resolver_version": 7,
        \\      "resolver_exists": true,
        \\      "sql": "INSERT INTO usage_records (id, status) VALUES ('u1', 'new') ON CONFLICT (id) DO UPDATE SET status = 'new'"
        \\    }
        \\  ]
        \\}
    ;
    var parsed_resolver = try std.json.parseFromSlice(std.json.Value, alloc, invalid_resolver_json, .{});
    defer parsed_resolver.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseSourceCorpusRootAlloc(alloc, parsed_resolver.value));
}

test "sql adapter corpus encodes fixture roots and entries" {
    const alloc = std.testing.allocator;
    const entries = [_]AppParityFixtureEncodedEntry{.{
        .entry = .{
            .name = "insert returning",
            .family = .insert,
            .summary = .{
                .table_name = "usage_records",
                .returning = 1,
                .returning_all = true,
            },
            .plan = "insert:table=usage_records:returning_rows=1:returning_all=1",
            .apply_setup_sql = &.{"CREATE TABLE usage_records (id text PRIMARY KEY)"},
            .returning_rows = &.{"{\"id\":\"u1\"}"},
            .resolver_row_json = "{\"id\":\"u1\"}",
            .resolver_version = 7,
            .resolver_exists = true,
            .source_schema_json = "{\"source\":true}",
            .params = &.{.{ .string = "u1" }},
            .sql = "INSERT INTO usage_records (id) VALUES ($1) RETURNING *",
        },
        .applied_plan = "applied:rebuild=false:validation=false:rewrite=false",
    }};
    const skipped = [_][]const u8{"unsupported generated expression"};
    const encoded = try fixtureJsonAlloc(
        alloc,
        "{\"version\":1}",
        2,
        &entries,
        &skipped,
    );
    defer alloc.free(encoded);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    const root = try parseFixtureRootAlloc(alloc, parsed.value);
    defer freeFixtureRoot(alloc, root);
    try std.testing.expectEqual(@as(usize, 2), root.source_entry_count);
    try std.testing.expectEqual(@as(usize, 1), root.entry_count);
    try std.testing.expectEqualStrings("unsupported generated expression", root.skipped_entries[0]);
    try std.testing.expectEqualStrings("{\"version\":1}", root.schema_json);

    const entry = try parseFixtureEntryAlloc(alloc, root.entries[0]);
    defer freeFixtureEntry(alloc, entry);
    try std.testing.expectEqual(AppParityCorpusPlanFamily.insert, entry.family);
    try std.testing.expectEqualStrings("usage_records", entry.summary.table_name.?);
    try std.testing.expectEqual(@as(?usize, 1), entry.summary.returning);
    try std.testing.expectEqual(@as(?bool, true), entry.summary.returning_all);
    try std.testing.expectEqualStrings("applied:rebuild=false:validation=false:rewrite=false", entry.applied_plan);
    try std.testing.expectEqual(@as(usize, 1), entry.params.len);
    try std.testing.expectEqualStrings("u1", entry.params[0].string);
}

test "sql adapter corpus owns fixture gate mode selection" {
    try std.testing.expect((try fixtureGateModeFromPaths(null, null)) == .none);
    switch (try fixtureGateModeFromPaths("fixture.json", null)) {
        .promote => |path| try std.testing.expectEqualStrings("fixture.json", path),
        else => return error.TestUnexpectedResult,
    }
    switch (try fixtureGateModeFromPaths(null, "fixture.json")) {
        .check => |path| try std.testing.expectEqualStrings("fixture.json", path),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectError(error.TestUnexpectedResult, fixtureGateModeFromPaths("promote.json", "check.json"));
}

test "sql adapter corpus detects empty and non-table fixture summaries" {
    try std.testing.expect(!summaryHasFields(.{}));
    try std.testing.expect(!summaryHasNonTableFields(.{ .table_name = "usage_records" }));
    try std.testing.expect(summaryHasFields(.{ .table_name = "usage_records" }));
    try std.testing.expect(summaryHasFields(.{ .predicates = 1 }));
    try std.testing.expect(summaryHasNonTableFields(.{ .table_name = "usage_records", .predicates = 1 }));
    try std.testing.expect(summaryHasNonTableFields(.{ .ddl_tag = .create_table }));
    try std.testing.expect(summaryHasNonTableFields(.{ .temporal_foreign_keys = 1 }));
}

test "sql adapter corpus owns fixture family policies" {
    try std.testing.expectEqual(UnsupportedPlanFamily.query, corpusUnsupportedPlanFamily(.unsupported).?);
    try std.testing.expectEqual(UnsupportedPlanFamily.update_joined_source, corpusUnsupportedPlanFamily(.unsupported_update_joined_source).?);
    try std.testing.expect(corpusPlanFamilyIsUnsupported(.unsupported_delete));
    try std.testing.expect(!corpusPlanFamilyIsUnsupported(.delete));

    try std.testing.expect(corpusFixtureFamilyNeedsReason(.adapter_noop_ddl));
    try std.testing.expect(corpusFixtureFamilyNeedsReason(.unsupported_write));
    try std.testing.expect(!corpusFixtureFamilyNeedsReason(.insert));
    try std.testing.expect(corpusStableReasonToken("multi_table_generation_barrier"));
    try std.testing.expect(!corpusStableReasonToken("future_unknown_reason"));
    try std.testing.expect(corpusPlanMatchesReason(.unsupported_write, "unsupported:write:requires=multi_table_generation_barrier", "multi_table_generation_barrier"));
    try std.testing.expect(!corpusPlanMatchesReason(.unsupported_write, "unsupported:write:requires=session_setting", "session_setting"));
    try std.testing.expect(corpusPlanMatchesReason(.adapter_noop_ddl, "adapter_noop:ddl:reason=session_setting", "session_setting"));
    try std.testing.expect(corpusPlanMatchesFamily(.insert_source, "insert_source:table=usage_records"));
    try std.testing.expect(!corpusPlanMatchesFamily(.insert_source, "insert:table=usage_records"));

    try std.testing.expect(corpusFixtureFamilyNeedsTableSummary(.update_source));
    try std.testing.expect(!corpusFixtureFamilyNeedsTableSummary(.ddl));
    try std.testing.expect(corpusFixtureFamilyAllowsSummary(.join));
    try std.testing.expect(!corpusFixtureFamilyAllowsSummary(.unsupported_read));

    try std.testing.expect(corpusReadPlanHasPrefix(.{ .family = .read, .plan = "read:query:table=usage_records" }, "read:query:"));
    try std.testing.expect(corpusReadPlanHasPrefix(.{ .family = .explain, .plan = "explain:kind=read:inner=read:query:table=usage_records" }, "read:query:"));
    try std.testing.expect(!corpusReadPlanHasPrefix(.{ .family = .read, .plan = "read:aggregate:table=usage_records" }, "read:query:"));
    try std.testing.expect(corpusExplainWriteInnerHasPrefix(.{ .family = .explain, .plan = "explain:kind=write:inner=insert:table=usage_records" }, ":inner=insert:"));
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(.{ .family = .explain, .plan = "explain:kind=read:inner=insert:table=usage_records" }, ":inner=insert:"));

    try std.testing.expect(corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records", ":having_expr=", 0));
    try std.testing.expect(corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records:having_expr=2", ":having_expr=", 2));
    try std.testing.expect(!corpusOptionalZeroSummaryMatchesPlan("aggregate:table=usage_records:having_expr=2", ":having_expr=", 0));
    try std.testing.expect(corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records", ":select_all=", false));
    try std.testing.expect(corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records:select_all=1", ":select_all=", true));
    try std.testing.expect(!corpusOptionalBool01SummaryMatchesPlan("query:table=usage_records:select_all=0", ":select_all=", true));

    try std.testing.expect(corpusFixtureHasAccessSummary(.{ .json_contains = 1 }));
    try std.testing.expect(!corpusFixtureHasAccessSummary(.{ .table_name = "usage_records" }));
    try std.testing.expect(corpusFixtureHasTemporalDdlSummary(.{ .summary = .{ .temporal_foreign_keys = 1 } }));
    try std.testing.expect(!corpusFixtureHasTemporalDdlSummary(.{ .summary = .{ .ddl_tag = .create_table } }));
    try std.testing.expect(corpusFixturePlanMatchesSourceTable(
        .{ .family = .insert_source, .plan = "insert_source:table=usage_records:source_table=usage_sources" },
        "usage_sources",
    ));
    try std.testing.expect(!corpusFixturePlanMatchesSourceTable(
        .{ .family = .insert_source, .plan = "insert_source:table=usage_records:source_table=usage_sources" },
        "other_sources",
    ));
    try std.testing.expect(corpusFixtureSqlParameterCoverageMatches(.{
        .family = .query,
        .sql = "SELECT id FROM usage_records WHERE tenant_id = $1",
        .params = &.{.{ .string = "tenant-a" }},
    }));
    try std.testing.expect(corpusFixtureSqlParameterCoverageMatches(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .prepare_statement },
        .plan = "ddl:prepare:params=2",
        .sql = "PREPARE lookup AS SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $2",
    }));
    try std.testing.expect(!corpusFixtureSqlParameterCoverageMatches(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .prepare_statement },
        .plan = "ddl:prepare:params=2",
        .sql = "PREPARE lookup AS SELECT id FROM usage_records WHERE tenant_id = $1",
    }));
}

test "sql adapter corpus rejects malformed fixture root metadata" {
    const alloc = std.testing.allocator;
    const mismatched_count_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 1,
        \\  "entry_count": 2,
        \\  "skipped_entries": [],
        \\  "schema_json": "{}",
        \\  "entries": [
        \\    {"name": "read", "family": "read", "plan": "read:table=usage_records", "sql": "SELECT * FROM usage_records"}
        \\  ]
        \\}
    ;
    var parsed_count = try std.json.parseFromSlice(std.json.Value, alloc, mismatched_count_json, .{});
    defer parsed_count.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureRootAlloc(alloc, parsed_count.value));

    const unknown_root_key_json =
        \\{
        \\  "fixture_format": 1,
        \\  "source_entry_count": 0,
        \\  "entry_count": 0,
        \\  "skipped_entries": [],
        \\  "schema_json": "{}",
        \\  "entries": [],
        \\  "unexpected": true
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_root_key_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureRootAlloc(alloc, parsed_unknown.value));
}

test "sql adapter corpus rejects fixture unknown keys and malformed scalars" {
    const alloc = std.testing.allocator;
    const unknown_key_json =
        \\{
        \\  "name": "bad",
        \\  "family": "read",
        \\  "plan": "read:table=usage_records",
        \\  "sql": "SELECT * FROM usage_records",
        \\  "unexpected": true
        \\}
    ;
    var parsed_unknown = try std.json.parseFromSlice(std.json.Value, alloc, unknown_key_json, .{});
    defer parsed_unknown.deinit();
    try std.testing.expectError(error.TestUnexpectedResult, parseFixtureEntryAlloc(alloc, parsed_unknown.value));

    const object = try fixtureJsonObject(parsed_unknown.value);
    try std.testing.expectError(error.TestUnexpectedResult, fixtureJsonOptionalUsize(object, "unexpected"));
}

test "sql adapter corpus fingerprints unsupported and adapter no-op reasons" {
    const alloc = std.testing.allocator;
    const unsupported = try unsupportedFingerprintAlloc(alloc, .write, .multi_table_generation_barrier);
    defer alloc.free(unsupported);
    try std.testing.expectEqualStrings("unsupported:write:requires=multi_table_generation_barrier", unsupported);
    try std.testing.expect(unsupportedPlanMatchesReason(unsupported, .write, .multi_table_generation_barrier));
    try std.testing.expect(!unsupportedPlanMatchesReason(unsupported, .write, .session_setting));
    try std.testing.expect(!unsupportedPlanMatchesReason("unsupported:write:requires=multi_table_generation_barrier_extra", .write, .multi_table_generation_barrier));

    const noop = try adapterNoopFingerprintAlloc(alloc, "ddl", .session_setting);
    defer alloc.free(noop);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=session_setting", noop);
    try std.testing.expect(adapterNoopPlanMatchesReason(noop, "ddl", .session_setting));
    try std.testing.expect(!adapterNoopPlanMatchesReason(noop, "ddl", .set_operation_plan));
    try std.testing.expect(!adapterNoopPlanMatchesReason("adapter_noop:ddl:reason=session_setting_extra", "ddl", .session_setting));

    try std.testing.expectError(error.UnsupportedSqlShape, unsupportedFingerprintAlloc(alloc, .write, .session_setting));
    try std.testing.expectError(error.UnsupportedSqlShape, adapterNoopFingerprintAlloc(alloc, "ddl", .set_operation_plan));
}

test "sql adapter corpus appends owned fingerprint fields" {
    const alloc = std.testing.allocator;

    var fingerprint = try alloc.dupe(u8, "query:table=usage_records");
    fingerprint = try appendNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "pred", 2);
    fingerprint = try appendNonZeroU32FingerprintAlloc(alloc, fingerprint, "offset", 4);
    fingerprint = try appendNamedNonZeroUsizeFingerprintAlloc(alloc, fingerprint, "left", "expr", 1);
    fingerprint = try appendTrueBoolFingerprintAlloc(alloc, fingerprint, "select_all", true);
    fingerprint = try appendBoolFingerprintAlloc(alloc, fingerprint, "verbose", false);
    fingerprint = try appendStringFingerprintAlloc(alloc, fingerprint, "claim", "skip_locked");
    defer alloc.free(fingerprint);

    try std.testing.expectEqualStrings(
        "query:table=usage_records:pred=2:offset=4:left_expr=1:select_all=1:verbose=0:claim=skip_locked",
        fingerprint,
    );

    var unchanged = try alloc.dupe(u8, "query:table=usage_records");
    unchanged = try appendNonZeroUsizeFingerprintAlloc(alloc, unchanged, "pred", 0);
    unchanged = try appendTrueBoolFingerprintAlloc(alloc, unchanged, "select_all", false);
    defer alloc.free(unchanged);
    try std.testing.expectEqualStrings("query:table=usage_records", unchanged);
}

test "sql adapter corpus string token matching is exact and unique" {
    const plan = "query:table=usage_records:claim=no_key_update_nowait:limit=none";
    try std.testing.expect(planHasExactStringToken(plan, ":claim=", "no_key_update_nowait"));
    try std.testing.expect(!planHasExactStringToken(plan, ":claim=", "no_key_update"));
    try std.testing.expect(planHasStringToken(plan, ":limit="));
    try std.testing.expect(planTokenAbsent(plan, ":offset="));
    try std.testing.expect(planHasAnyExactStringToken(plan, ":claim=", &.{
        "no_key_update",
        "no_key_update_nowait",
    }));

    const duplicate = "read:query:table=usage_records:table=usage_records";
    try std.testing.expect(!planHasExactStringToken(duplicate, ":table=", "usage_records"));
    try std.testing.expect(!planHasStringToken(duplicate, ":table="));
}

test "sql adapter corpus numeric and bool token matching is exact and unique" {
    const plan = "applied:rebuild=true:validation=false:rewrite=false:unvalidated_unique=10:unvalidated_fk=1:expr=0:generated_expr=1:kind=none:setting=search_path";
    try std.testing.expect(planHasExactBoolToken(plan, "rebuild=", true));
    try std.testing.expect(planHasExactBoolToken(plan, "validation=", false));
    try std.testing.expectEqual(@as(?usize, 10), planUsizeTokenValue(plan, "unvalidated_unique="));
    try std.testing.expect(planHasExactUsizeToken(plan, "unvalidated_fk=", 1));
    try std.testing.expect(planHasNonZeroToken(plan, "unvalidated_unique="));
    try std.testing.expect(planHasNonZeroUsizeTokenNamePrefix(plan, "unvalidated_"));
    try std.testing.expectEqual(@as(?usize, 0), planUsizeOptionalTokenValue(plan, "missing="));
    try std.testing.expect(planUsizeTokenSumMatches(plan, &.{ "unvalidated_unique=", "unvalidated_fk=" }, 11));
    try std.testing.expect(planUsizeOptionalTokenSumMatches(plan, &.{ "unvalidated_fk=", "missing=", "generated_expr=" }, 2));
    try std.testing.expect(planBoolTokenSumMatches(plan, &.{ "rebuild=", "validation=", "rewrite=" }, 1));
    try std.testing.expect(planNonNoneStringTokenSumMatches(plan, &.{ "kind=", "setting=" }, 1));
    try std.testing.expect(planHasAnyNonZeroToken(plan, &.{ "expr=", "generated_expr=" }));
    try std.testing.expect(!planHasAnyNonZeroToken(plan, &.{ "expr=", "missing=" }));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=10x", "pred=", 10));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=1:pred=1", "pred=", 1));
    try std.testing.expect(!planHasExactBoolToken("ddl:replace=true_extra", "replace=", true));
}

test "sql adapter corpus plan predicates are exact and structured" {
    const applied = "applied:rebuild=true:validation=false:rewrite=false:building_indexes=0:unvalidated_unique=10:unvalidated_fk=1:unvalidated_check=0:update_policy=0";
    try std.testing.expect(appliedPlanIsStructured(applied));
    try std.testing.expect(appliedPlanHasExactBoolToken(applied, "rebuild=", true));
    try std.testing.expect(appliedPlanHasExactBoolToken(applied, "validation=", false));
    try std.testing.expect(appliedPlanHasExactUsizeToken(applied, "unvalidated_unique=", 10));
    try std.testing.expect(!appliedPlanHasExactUsizeToken(applied, "unvalidated_unique=", 1));
    try std.testing.expect(!appliedPlanHasExactBoolToken("applied:rebuild=true:rewrite=false", "rebuild=", true));
    try std.testing.expect(appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true"));
    try std.testing.expect(!appliedPlanIsStructured("applied:drop_table:rebuild=true:validation=true:rewrite=true:extra=1"));

    const explain = "explain:kind=write:analyze=false:inner=insert:table=usage_records:writes=1:transforms=0";
    try std.testing.expect(explainPlanHasKind(explain, "write"));
    try std.testing.expect(!explainPlanHasKind(explain, "read"));
    try std.testing.expect(explainPlanInnerStartsWith(explain, "insert:"));
    try std.testing.expect(!explainPlanInnerStartsWith("explain:kind=write:inner=insert:inner=update:", "insert:"));

    try std.testing.expect(writePlanHasCounts("insert:table=usage_records:writes=2:transforms=1", 2, 1));
    try std.testing.expect(!writePlanHasCounts("insert:table=usage_records:writes=2:transforms=1", 1, 1));
    try std.testing.expect(joinedSourcePlanHasCounts("update_joined_source:table=usage_records:right_pred=1:on=2", 1, 2));
    try std.testing.expect(!joinedSourcePlanHasCounts("update_joined_source:table=usage_records:right_pred=1:on=2", 0, 2));
}

test "sql adapter corpus detects create-table empty-catalog applicability with exact bool tokens" {
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=false:pk=1",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=true:pk=1",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=false:replace=true:pk=1",
    }));
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table },
        .plan = "ddl:create_table:table=usage_records:columns=1:unique=0:fk=0:checks=0:if_not_exists=true_extra:pk=1",
    }));
}

test "sql adapter corpus exact-token helpers reject ddl submode suffixes" {
    const comment = "ddl:comment:kind=table_extra:object=users:comment=true";
    try std.testing.expect(!planHasExactStringToken(comment, ":kind=", "table"));
    try std.testing.expect(planHasExactStringToken(comment, ":kind=", "table_extra"));

    const transaction = "ddl:transaction_control:kind=transaction_mode:starter=start_transaction_extra:isolation=serializable:access=none:deferrable=none";
    try std.testing.expect(!planHasExactStringToken(transaction, ":starter=", "start_transaction"));
    try std.testing.expect(planHasExactStringToken(transaction, ":starter=", "start_transaction_extra"));

    const population = "relation_population:mode=create_table_as_extra:target=usage_archive:lifetime=durable:if_not_exists=false:source=read:query:query:table=usage_records";
    try std.testing.expect(!planHasExactStringToken(population, "relation_population:mode=", "create_table_as"));
    try std.testing.expect(planHasExactStringToken(population, "relation_population:mode=", "create_table_as_extra"));
}

test "sql adapter corpus explain wrapper predicates are exact" {
    const write_suffix = AppParityCorpusEntry{
        .family = .explain,
        .summary = .{ .returning = 1 },
        .plan = "explain:kind=write_extra:analyze=false:inner=insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0",
    };
    try std.testing.expect(!explainPlanHasKind(write_suffix.plan, "write"));
    try std.testing.expect(!corpusFixtureAllowsReturningSummary(write_suffix));
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(write_suffix, ":inner=insert:"));

    const duplicate_inner = AppParityCorpusEntry{
        .family = .explain,
        .summary = .{ .returning = 1 },
        .plan = "explain:kind=write:analyze=false:inner=insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0:inner=update_source:table=usage_records",
    };
    try std.testing.expect(!corpusExplainWriteInnerHasPrefix(duplicate_inner, ":inner=insert:"));

    const read_suffix = AppParityCorpusEntry{
        .family = .explain,
        .plan = "explain:kind=read_extra:analyze=false:inner=read:query:query:table=usage_records:ctes=0:pred=0:select=1:order=0:limit=none:claim=none",
    };
    try std.testing.expect(!corpusReadPlanHasPrefix(read_suffix, "read:query:"));

    const read_stray_prefix = AppParityCorpusEntry{
        .family = .explain,
        .plan = "explain:kind=read:analyze=false:detail=read:query:inner=read:join:join:type=inner:left=usage_records:right=customer_records:left_pred=0:right_pred=0:on=1:select=2:order=0:limit=none",
    };
    try std.testing.expect(!corpusReadPlanHasPrefix(read_stray_prefix, "read:query:"));

    const options = "explain:kind=read:analyze=true_extra:inner=read:query:query:table=usage_records:format=:verbose=1:costs=0";
    try std.testing.expect(explainPlanHasKind(options, "read"));
    try std.testing.expect(!planHasExactBoolToken(options, ":analyze=", true));
    try std.testing.expect(!planHasStringToken(options, ":format="));
    try std.testing.expect(planUsizeTokenValue(options, ":verbose=") != null);
    try std.testing.expect(planUsizeTokenValue(options, ":costs=") != null);
}

test "sql adapter corpus data-driven coverage regressions" {
    const alloc = std.testing.allocator;
    const fixture_json = @embedFile("../fixtures/sql_api_coverage_regressions.json");
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, fixture_json, .{});
    defer parsed.deinit();

    const root = try fixtureJsonObject(parsed.value);
    try fixtureRequireOnlyKeys(root, &.{ "coverage_format", "cases" });
    const coverage_format = try fixtureJsonOptionalU64(root, "coverage_format", 0);
    if (coverage_format != app_parity_coverage_fixture_format) return error.TestUnexpectedResult;
    const cases = switch (root.get("cases") orelse return error.TestUnexpectedResult) {
        .array => |array| array.items,
        else => return error.TestUnexpectedResult,
    };
    if (cases.len == 0) return error.TestUnexpectedResult;
    for (cases) |regression_case| {
        try checkCoverageRegressionCase(alloc, regression_case);
    }
}

test "sql adapter corpus validates fixture mutation and aggregate summaries" {
    const ddl_entry = AppParityCorpusEntry{
        .name = "create table",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .operations = 3 },
        .plan = "ddl:create_table:table=usage_records:columns=2:unique=1:fk=1:checks=1:if_not_exists=false:pk=1",
    };
    try std.testing.expect(corpusFixtureOperationsSummaryMatchesPlan(ddl_entry, 3));
    try std.testing.expect(!corpusFixtureOperationsSummaryMatchesPlan(ddl_entry, 2));

    const insert_entry = AppParityCorpusEntry{
        .name = "insert returning",
        .sql = "INSERT INTO usage_records (id) VALUES ('u1') RETURNING *",
        .family = .insert,
        .summary = .{ .returning = 1, .returning_all = true, .conflict_where = false },
        .plan = "insert:table=usage_records:writes=1:transforms=0:ops=0:deletes=0:returning_rows=1:returning_expr=0:returning_all=1",
    };
    try std.testing.expect(corpusFixtureAllowsReturningSummary(insert_entry));
    try std.testing.expect(corpusFixtureReturningSummaryMatchesPlan(insert_entry, 1));
    try std.testing.expect(corpusFixtureReturningAllSummaryMatchesPlan(insert_entry, true));
    try std.testing.expect(corpusFixtureAllowsConflictWhereSummary(insert_entry));
    try std.testing.expect(corpusFixtureConflictWhereSummaryMatchesPlan(insert_entry, false));

    const insert_source_entry = AppParityCorpusEntry{
        .name = "insert source conflict transform",
        .sql = "INSERT INTO usage_records SELECT * FROM staged_records ON CONFLICT DO UPDATE SET count = count + 1",
        .family = .insert_source,
        .summary = .{ .patch_expressions = 1, .increment_expressions = 1, .json_set_expressions = 1 },
        .plan = "insert_source:table=usage_records:source_table=staged_records:assignments=2:conflict=1:returning=0:returning_expr=0:returning_all=0:conflict_patch_expr=1:conflict_increment_expr=1:conflict_json_set_expr=1",
    };
    try std.testing.expect(corpusFixtureAllowsMutationTransformSummary(insert_source_entry));
    try std.testing.expect(corpusFixtureTransformSummaryMatchesPlan(insert_source_entry));

    const joined_update_entry = AppParityCorpusEntry{
        .name = "joined update source assignments",
        .sql = "UPDATE usage_records SET status = staged.status FROM staged_records staged WHERE usage_records.id = staged.id",
        .family = .update_joined_source,
        .summary = .{ .source_assignments = 2 },
        .plan = "update_joined_source:target=usage_records:source=staged_records:left_pred=0:right_pred=0:on=1:ops=1:source_assignments=2:returning=0:returning_expr=0",
    };
    try std.testing.expect(corpusFixtureAllowsSourceAssignmentsSummary(joined_update_entry));
    try std.testing.expect(corpusFixtureSourceAssignmentsSummaryMatchesPlan(joined_update_entry, 2));

    const merge_entry = AppParityCorpusEntry{
        .name = "merge arms",
        .sql = "MERGE INTO usage_records USING staged_records ON usage_records.id = staged_records.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN DO NOTHING",
        .family = .merge_mutation,
        .summary = .{ .matched_predicates = 1, .matched_delete = true, .matched_do_nothing = false, .not_matched_predicates = 0, .not_matched_do_nothing = true },
        .plan = "merge_mutation:target=usage_records:source=staged_records:match=1:matched_pred=1:matched_update=0:matched_delete=1:matched_noop=0:not_matched_pred=0:not_matched_insert=0:not_matched_noop=1:returning=0:returning_expr=0:returning_all=0",
    };
    try std.testing.expect(corpusFixtureAllowsMergeArmSummary(merge_entry));
    try std.testing.expect(corpusFixtureMergeArmSummaryMatchesPlan(merge_entry));

    const aggregate_entry = AppParityCorpusEntry{
        .name = "aggregate",
        .sql = "SELECT tenant_id, count(*) FROM usage_records GROUP BY tenant_id",
        .family = .aggregate,
        .summary = .{ .group_by = 1, .group_expressions = 0, .aggregations = 1, .filter_groups = 0, .having = 0, .having_expressions = 0, .having_any = 0, .having_not = 0 },
        .plan = "aggregate:table=usage_records:group=1:group_expr=0:aggs=1:having=0",
    };
    try std.testing.expect(corpusFixtureAllowsAggregateSummary(aggregate_entry));
    try std.testing.expect(corpusFixtureAggregateSummaryMatchesPlan(aggregate_entry));
}

test "sql adapter corpus validates ddl select and predicate summaries" {
    const create_index_entry = AppParityCorpusEntry{
        .name = "create index expression",
        .sql = "CREATE INDEX usage_records_expr_idx ON usage_records (status, lower(id)) WHERE status = 'active'",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .select = 2, .predicates = 1 },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=1:generated_expr=0:where=1:unique=false:if_not_exists=false",
    };
    try std.testing.expect(corpusFixtureDdlSelectSummaryMatchesPlan(create_index_entry));
    try std.testing.expect(corpusFixtureDdlPredicateSummaryMatchesPlan(create_index_entry));

    const stale_select_entry = AppParityCorpusEntry{
        .name = "stale index expression",
        .sql = create_index_entry.sql,
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .select = 2, .predicates = 1 },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=1:unique=false:if_not_exists=false",
    };
    try std.testing.expect(!corpusFixtureDdlSelectSummaryMatchesPlan(stale_select_entry));
    try std.testing.expect(corpusFixtureDdlPredicateSummaryMatchesPlan(stale_select_entry));

    const stale_predicate_entry = AppParityCorpusEntry{
        .name = "stale index predicate",
        .sql = create_index_entry.sql,
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .select = 2, .predicates = 1 },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=1:generated_expr=0:where=0:unique=false:if_not_exists=false",
    };
    try std.testing.expect(corpusFixtureDdlSelectSummaryMatchesPlan(stale_predicate_entry));
    try std.testing.expect(!corpusFixtureDdlPredicateSummaryMatchesPlan(stale_predicate_entry));

    const session_entry = AppParityCorpusEntry{
        .name = "session ddl without select",
        .sql = "DISCARD ALL",
        .family = .ddl,
        .summary = .{ .ddl_tag = .discard_all },
        .plan = "ddl:session:discard_all",
    };
    try std.testing.expect(corpusFixtureDdlSelectSummaryMatchesPlan(session_entry));
    try std.testing.expect(corpusFixtureDdlPredicateSummaryMatchesPlan(session_entry));
}

test "sql adapter corpus validates read shape fixture summaries" {
    const query_entry = AppParityCorpusEntry{
        .name = "query summaries",
        .sql = "SELECT DISTINCT ON (tenant_id) id, status FROM usage_records WHERE attrs @> '{}' ORDER BY tenant_id LIMIT 10 OFFSET 2 FOR UPDATE SKIP LOCKED",
        .family = .query,
        .summary = .{
            .predicates = 1,
            .json_contains = 1,
            .select = 2,
            .select_all = true,
            .distinct_on = 1,
            .order_by = 1,
            .limit = 10,
            .offset = 2,
            .row_claim_skip_locked = true,
        },
        .plan = "query:table=usage_records:pred=1:json_contains=1:select=2:select_all=1:distinct_on=1:order=1:limit=10:offset=2:claim=skip_locked",
    };
    try std.testing.expect(corpusFixtureAllowsPredicateSummary(query_entry));
    try std.testing.expect(corpusFixturePredicateSummaryMatchesPlan(query_entry));
    try std.testing.expect(corpusFixtureAllowsAccessSummary(query_entry));
    try std.testing.expect(corpusFixtureAccessSummaryMatchesPlan(query_entry));
    try std.testing.expect(corpusFixtureSelectSummaryMatchesPlan(query_entry));
    try std.testing.expect(corpusFixtureAllowsFullQueryOutputSummary(query_entry));
    try std.testing.expect(corpusFixtureFullQueryOutputSummaryMatchesPlan(query_entry));
    try std.testing.expect(corpusFixtureAllowsPaginationSummary(query_entry));
    try std.testing.expect(corpusFixturePaginationSummaryMatchesPlan(query_entry));
    try std.testing.expect(corpusFixtureAllowsRowClaimSummary(query_entry));
    try std.testing.expect(corpusFixtureRowClaimSummaryMatchesPlan(query_entry));

    const join_entry = AppParityCorpusEntry{
        .name = "join summaries",
        .sql = "SELECT usage_records.id FROM usage_records JOIN users ON usage_records.user_id = users.id",
        .family = .join,
        .summary = .{
            .predicates = 2,
            .text_patterns = 3,
            .join_select = 1,
            .join_on = 1,
            .row_claim_skip_locked = false,
        },
        .plan = "join:left=usage_records:right=users:left_pred=1:right_pred=1:left_text=1:right_text=2:select=1:on=1:claim=nowait",
    };
    try std.testing.expect(corpusFixtureAllowsPredicateSummary(join_entry));
    try std.testing.expect(corpusFixturePredicateSummaryMatchesPlan(join_entry));
    try std.testing.expect(corpusFixtureAllowsAccessSummary(join_entry));
    try std.testing.expect(corpusFixtureAccessSummaryMatchesPlan(join_entry));
    try std.testing.expect(corpusFixtureAllowsJoinSelectSummary(join_entry));
    try std.testing.expect(corpusFixtureJoinSelectSummaryMatchesPlan(join_entry, 1));
    try std.testing.expect(corpusFixtureAllowsJoinOnSummary(join_entry));
    try std.testing.expect(corpusFixtureJoinOnSummaryMatchesPlan(join_entry, 1));
    try std.testing.expect(corpusFixtureAllowsRowClaimSummary(join_entry));
    try std.testing.expect(corpusFixtureRowClaimSummaryMatchesPlan(join_entry));

    const read_window_entry = AppParityCorpusEntry{
        .name = "wrapped window summaries",
        .sql = "SELECT id, row_number() OVER (PARTITION BY tenant_id) FROM usage_records",
        .family = .read,
        .summary = .{
            .predicates = 1,
            .json_path_eq = 1,
            .select = 2,
            .windows = 1,
        },
        .plan = "read:window:table=usage_records:source_pred=1:source_json_eq=1:select=2:windows=1",
    };
    try std.testing.expect(corpusFixtureAllowsPredicateSummary(read_window_entry));
    try std.testing.expect(corpusFixturePredicateSummaryMatchesPlan(read_window_entry));
    try std.testing.expect(corpusFixtureAllowsAccessSummary(read_window_entry));
    try std.testing.expect(corpusFixtureAccessSummaryMatchesPlan(read_window_entry));
    try std.testing.expect(corpusFixtureSelectSummaryMatchesPlan(read_window_entry));
    try std.testing.expect(corpusFixtureAllowsWindowSummary(read_window_entry));
    try std.testing.expect(corpusFixtureWindowSummaryMatchesPlan(read_window_entry, 1));

    const lateral_entry = AppParityCorpusEntry{
        .name = "lateral summaries",
        .sql = "SELECT usage_records.id FROM usage_records JOIN LATERAL (...) l ON true",
        .family = .lateral,
        .summary = .{
            .expression_array_contains = 2,
            .join_select = 1,
            .lateral_correlations = 1,
            .right_offset = 4,
        },
        .plan = "lateral:left=usage_records:right=usage_events:left_expr_array=1:right_expr_array=1:select=1:corr=1:right_offset=4",
    };
    try std.testing.expect(corpusFixtureAllowsAccessSummary(lateral_entry));
    try std.testing.expect(corpusFixtureAccessSummaryMatchesPlan(lateral_entry));
    try std.testing.expect(corpusFixtureAllowsJoinSelectSummary(lateral_entry));
    try std.testing.expect(corpusFixtureJoinSelectSummaryMatchesPlan(lateral_entry, 1));
    try std.testing.expect(corpusFixtureAllowsLateralSummary(lateral_entry));
    try std.testing.expect(corpusFixtureLateralSummaryMatchesPlan(lateral_entry));

    const bad_pagination_entry = AppParityCorpusEntry{
        .name = "stale pagination",
        .sql = query_entry.sql,
        .family = .query,
        .summary = .{ .limit = 5 },
        .plan = "query:table=usage_records:limit=10",
    };
    try std.testing.expect(corpusFixtureAllowsPaginationSummary(bad_pagination_entry));
    try std.testing.expect(!corpusFixturePaginationSummaryMatchesPlan(bad_pagination_entry));
}

fn appParitySqlHasComputedPattern(sql: []const u8) bool {
    return std.mem.indexOf(u8, sql, "lower(") != null and
        (std.mem.indexOf(u8, sql, " LIKE ") != null or std.mem.indexOf(u8, sql, " ILIKE ") != null);
}

fn appParityAnyStringContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.indexOf(u8, value, needle) != null) return true;
    }
    return false;
}

pub const AppParityCorpusCoverage = struct {
    ddl: bool = false,
    ddl_table_clone: bool = false,
    ddl_view_create: bool = false,
    ddl_view_rename: bool = false,
    ddl_view_drop: bool = false,
    ddl_materialized_view_create: bool = false,
    ddl_materialized_view_refresh: bool = false,
    ddl_materialized_view_drop: bool = false,
    ddl_relation_lifetime_temporary: bool = false,
    ddl_relation_lifetime_unlogged: bool = false,
    ddl_enum_type_create: bool = false,
    ddl_enum_type_add_value: bool = false,
    ddl_enum_type_drop: bool = false,
    ddl_domain_create: bool = false,
    ddl_domain_alter: bool = false,
    ddl_domain_drop: bool = false,
    ddl_sequence_create: bool = false,
    ddl_sequence_create_typed_owned: bool = false,
    ddl_sequence_alter: bool = false,
    ddl_sequence_alter_typed_owned: bool = false,
    ddl_sequence_drop: bool = false,
    ddl_identity_allocator_serial: bool = false,
    ddl_identity_allocator_generated: bool = false,
    ddl_identity_allocator_generated_options: bool = false,
    ddl_schema_namespace_create: bool = false,
    ddl_schema_namespace_rename: bool = false,
    ddl_schema_namespace_drop: bool = false,
    ddl_extension_create: bool = false,
    ddl_function_create: bool = false,
    ddl_function_replace: bool = false,
    ddl_function_drop: bool = false,
    ddl_procedure_create: bool = false,
    ddl_procedure_drop: bool = false,
    ddl_role_create: bool = false,
    ddl_role_alter: bool = false,
    ddl_role_drop: bool = false,
    ddl_privilege_grant: bool = false,
    ddl_privilege_revoke: bool = false,
    ddl_copy_from: bool = false,
    ddl_copy_to: bool = false,
    ddl_partition_create_parent: bool = false,
    ddl_partition_create_child: bool = false,
    ddl_partition_attach: bool = false,
    ddl_partition_detach: bool = false,
    ddl_row_security_enable: bool = false,
    ddl_row_security_disable: bool = false,
    ddl_row_security_create_policy: bool = false,
    ddl_row_security_drop_policy: bool = false,
    ddl_database_create: bool = false,
    ddl_database_alter: bool = false,
    ddl_database_drop: bool = false,
    ddl_tablespace_create: bool = false,
    ddl_tablespace_rename: bool = false,
    ddl_tablespace_drop: bool = false,
    ddl_notification_listen: bool = false,
    ddl_notification_notify: bool = false,
    ddl_notification_unlisten: bool = false,
    ddl_publication_create: bool = false,
    ddl_publication_alter: bool = false,
    ddl_publication_drop: bool = false,
    ddl_subscription_create: bool = false,
    ddl_subscription_alter: bool = false,
    ddl_subscription_drop: bool = false,
    ddl_collation_create: bool = false,
    ddl_collation_rename: bool = false,
    ddl_collation_drop: bool = false,
    ddl_operator_create: bool = false,
    ddl_operator_drop: bool = false,
    ddl_aggregate_create: bool = false,
    ddl_aggregate_drop: bool = false,
    ddl_cast_create: bool = false,
    ddl_cast_drop: bool = false,
    ddl_vacuum_maintenance: bool = false,
    ddl_analyze_maintenance: bool = false,
    ddl_reindex_maintenance: bool = false,
    ddl_cluster_maintenance: bool = false,
    ddl_prepare_statement: bool = false,
    ddl_prepare_cte_write_statement: bool = false,
    ddl_execute_statement: bool = false,
    ddl_deallocate_statement: bool = false,
    ddl_declare_cursor: bool = false,
    ddl_fetch_cursor: bool = false,
    ddl_close_cursor: bool = false,
    ddl_savepoint_transaction: bool = false,
    ddl_release_savepoint: bool = false,
    ddl_rollback_to_savepoint: bool = false,
    ddl_comment_table: bool = false,
    ddl_comment_column: bool = false,
    ddl_comment_index: bool = false,
    ddl_comment_constraint: bool = false,
    ddl_table_lock: bool = false,
    ddl_constraint_mode: bool = false,
    ddl_set_transaction_mode: bool = false,
    ddl_start_transaction_mode: bool = false,
    ddl_begin_transaction_mode: bool = false,
    ddl_transaction_deferrable_true: bool = false,
    ddl_transaction_deferrable_false: bool = false,
    ddl_advisory_lock: bool = false,
    ddl_advisory_unlock: bool = false,
    read: bool = false,
    read_query: bool = false,
    read_aggregate: bool = false,
    read_join: bool = false,
    read_lateral: bool = false,
    read_window: bool = false,
    read_cte_query_expression: bool = false,
    read_cte_aggregate_expression: bool = false,
    read_cte_window_expression: bool = false,
    read_join_cross_table_source_schema_classifier: bool = false,
    read_lateral_cross_table_source_schema_classifier: bool = false,
    query: bool = false,
    aggregate: bool = false,
    join: bool = false,
    lateral: bool = false,
    window: bool = false,
    explain: bool = false,
    explain_options: bool = false,
    explain_analyze: bool = false,
    explain_write: bool = false,
    relation_population_select_into: bool = false,
    relation_population_create_table_as: bool = false,
    insert: bool = false,
    insert_source: bool = false,
    insert_source_expression_assignment: bool = false,
    insert_source_regexp_expression_assignment: bool = false,
    insert_source_computed_pattern_source: bool = false,
    insert_source_expression_or_source: bool = false,
    insert_source_expression_not_source: bool = false,
    insert_source_returning_all_expression: bool = false,
    insert_source_conflict_default_update: bool = false,
    insert_source_conflict_json_set_expression: bool = false,
    insert_source_conflict_regexp_expression: bool = false,
    insert_source_conflict_boolean_is_not_guard: bool = false,
    update: bool = false,
    delete: bool = false,
    update_source: bool = false,
    delete_source: bool = false,
    truncate_source: bool = false,
    update_joined_source: bool = false,
    update_joined_source_cte_mutation: bool = false,
    delete_joined_source: bool = false,
    delete_joined_source_cte_mutation: bool = false,
    adapter_noop_ddl: bool = false,
    unsupported_query: bool = false,
    unsupported_read: bool = false,
    unsupported_ddl: bool = false,
    ddl_temporal_fk_delete_set_null_action: bool = false,
    ddl_temporal_fk_delete_cascade_action: bool = false,
    unsupported_ddl_temporal_fk_update_action: bool = false,
    unsupported_ddl_prepare_recursive_cte_statement: bool = false,
    unsupported_ddl_deferrable_unique_constraint: bool = false,
    unsupported_ddl_deferrable_primary_key: bool = false,
    unsupported_ddl_transaction_scoped_search_path: bool = false,
    unsupported_write: bool = false,
    unsupported_write_recursive_cte_insert: bool = false,
    unsupported_write_recursive_cte_update: bool = false,
    unsupported_write_recursive_cte_delete: bool = false,
    unsupported_write_recursive_cte_merge: bool = false,
    unsupported_insert: bool = false,
    unsupported_update: bool = false,
    unsupported_update_source: bool = false,
    unsupported_delete: bool = false,
    unsupported_update_joined_source: bool = false,
    unsupported_delete_joined_source: bool = false,
    unsupported_merge_mutation: bool = false,
    unsupported_query_recursive_cte_stream_plan: bool = false,
    unsupported_query_set_operation_plan: bool = false,
    query_calendar_interval_expression: bool = false,
    unsupported_read_recursive_cte_stream_plan: bool = false,
    unsupported_read_duplicate_output_name: bool = false,
    unsupported_read_aggregate_duplicate_output_name: bool = false,
    unsupported_read_set_operation_union: bool = false,
    unsupported_read_set_operation_intersect: bool = false,
    unsupported_read_set_operation_except: bool = false,
    unsupported_read_ordered_set_aggregate_plan: bool = false,
    read_row_lock_nowait: bool = false,
    read_row_lock_share: bool = false,
    read_row_lock_key_share: bool = false,
    query_row_lock_no_key_update: bool = false,
    merge_mutation_typed_plan: bool = false,
    merge_mutation_default_expressions: bool = false,
    unsupported_write_truncate_multi_table: bool = false,
    unsupported_write_truncate_cascade: bool = false,
    truncate_continue_identity: bool = false,
    truncate_restart_identity: bool = false,
    update_source_claim_nowait: bool = false,
    update_source_claim_no_key_update: bool = false,
    unsupported_read_row_lock_target: bool = false,
    unsupported_update_source_row_lock_target: bool = false,
    unsupported_update_joined_source_row_lock_target: bool = false,
    unsupported_merge_mutation_cte: bool = false,
    update_identity_rewrite: bool = false,
    unsupported_update_non_unique_point_selector: bool = false,
    unsupported_delete_non_unique_point_selector: bool = false,
    unsupported_delete_multi_output_subquery_selector: bool = false,
    unsupported_update_joined_multi_output_subquery_selector: bool = false,
    unsupported_delete_joined_multi_output_subquery_selector: bool = false,
    insert_source_cross_table_source_schema: bool = false,
    joined_source_cross_table_source_schema: bool = false,
    read_join_cross_table_source_schema: bool = false,
    read_lateral_cross_table_source_schema: bool = false,
    merge_cross_table_source_schema: bool = false,
    scalar_membership: bool = false,
    boolean_is_predicate: bool = false,
    boolean_is_not_predicate: bool = false,
    boolean_unknown_predicate: bool = false,
    postfix_null_test_predicate: bool = false,
    expression_postfix_null_test_predicate: bool = false,
    json_access_path: bool = false,
    array_access_path: bool = false,
    text_pattern: bool = false,
    query_access_or_predicates: bool = false,
    query_array_overlap_access_or: bool = false,
    query_access_not_predicates: bool = false,
    expression_predicate: bool = false,
    query_computed_pattern_predicate: bool = false,
    mixed_scalar_expression_or: bool = false,
    expression_order: bool = false,
    query_order_using_operator: bool = false,
    aggregate_order_using_operator: bool = false,
    join_order_using_operator: bool = false,
    lateral_order_using_operator: bool = false,
    window_order_using_operator: bool = false,
    update_source_order_using_operator: bool = false,
    delete_source_order_using_operator: bool = false,
    query_fixed_interval_expression: bool = false,
    query_mixed_interval_expression: bool = false,
    query_now_expression: bool = false,
    query_current_timestamp_expression: bool = false,
    query_current_timestamp_precision_expression: bool = false,
    query_current_date_expression: bool = false,
    query_uuid_generation_expression: bool = false,
    query_uuid_generate_v4_expression: bool = false,
    cte_stream: bool = false,
    cte_query: bool = false,
    cte_aggregate: bool = false,
    cte_window: bool = false,
    catalog_setup_sql: bool = false,
    applied_catalog_plan: bool = false,
    applied_catalog_rebuild: bool = false,
    applied_catalog_validation: bool = false,
    applied_catalog_rewrite: bool = false,
    deterministic_returning_rows: usize = 0,
    deterministic_insert_returning_rows: bool = false,
    deterministic_update_returning_rows: bool = false,
    deterministic_delete_returning_rows: bool = false,
    insert_typed_datetime_literal: bool = false,
    returning_all_insert: bool = false,
    returning_all_update: bool = false,
    returning_all_delete: bool = false,
    returning_all_update_source: bool = false,
    returning_all_delete_source: bool = false,
    returning_all_update_joined_source: bool = false,
    returning_all_delete_joined_source: bool = false,
    conflict_do_nothing_returning_all: bool = false,
    conflict_do_update: bool = false,
    conflict_default_update: bool = false,
    conflict_coalesce_existing_update: bool = false,
    conflict_numeric_expression_update: bool = false,
    conflict_case_expression_update: bool = false,
    conflict_current_timestamp_precision: bool = false,
    conflict_current_date_update: bool = false,
    conflict_uuid_generation_update: bool = false,
    conflict_text_expression_update: bool = false,
    conflict_octet_length_expression_update: bool = false,
    conflict_bit_length_expression_update: bool = false,
    conflict_regexp_replace_expression_update: bool = false,
    conflict_regexp_match_expression_update: bool = false,
    conflict_regexp_count_expression_update: bool = false,
    conflict_regexp_instr_expression_update: bool = false,
    conflict_regexp_substr_expression_update: bool = false,
    conflict_jsonb_update: bool = false,
    conflict_jsonb_concat_update: bool = false,
    conflict_guard_where: bool = false,
    conflict_guard_where_skip: bool = false,
    conflict_returning_expression: bool = false,
    conflict_interval_update: bool = false,
    conflict_mixed_interval_update: bool = false,
    conflict_row_assignment: bool = false,
    conflict_row_assignment_default: bool = false,
    conflict_row_assignment_constructor: bool = false,
    conflict_boolean_expression_update: bool = false,
    update_source_boolean_expression_update: bool = false,
    update_joined_source_boolean_expression_update: bool = false,
    multi_row_insert: bool = false,
    multi_row_conflict_do_nothing: bool = false,
    multi_row_conflict_do_nothing_duplicate_target: bool = false,
    write_plan_insert_op_set: bool = false,
    write_plan_insert_op_inc: bool = false,
    write_plan_update_op_set: bool = false,
    write_plan_update_op_push: bool = false,
    write_plan_update_op_pull: bool = false,
    point_update_jsonb: bool = false,
    point_update_jsonb_concat: bool = false,
    point_update_array: bool = false,
    point_update_uuid_generation: bool = false,
    point_update_patch_expression: bool = false,
    update_source_claim_skip_locked: bool = false,
    update_source_pagination: bool = false,
    update_source_nullable_pagination: bool = false,
    update_source_boolean_is_not_predicate: bool = false,
    update_source_returning_expression: bool = false,
    point_update_expression_partial_unique_selector: bool = false,
    point_delete_expression_partial_unique_selector: bool = false,
    delete_source_fetch_pagination: bool = false,
    delete_source_nullable_pagination: bool = false,
    delete_source_boolean_unknown_predicate: bool = false,
    delete_source_returning_expression: bool = false,
    joined_source_ordered_pagination: bool = false,
    joined_source_expression_predicate: bool = false,
    joined_source_expression_group: bool = false,
    joined_source_expression_array: bool = false,
    joined_source_returning_expression: bool = false,
    joined_source_returning_source_field: bool = false,
    joined_source_returning_source_expression: bool = false,
    update_joined_source_returning_source_expression: bool = false,
    delete_joined_source_returning_source_expression: bool = false,
    update_joined_source_non_primary_semijoin: bool = false,
    delete_joined_source_non_primary_semijoin: bool = false,
    update_joined_source_correlated_semijoin: bool = false,
    delete_joined_source_correlated_semijoin: bool = false,
    update_joined_source_correlated_filtered_semijoin: bool = false,
    delete_joined_source_correlated_filtered_semijoin: bool = false,
    update_joined_source_semijoin_match_expression: bool = false,
    delete_joined_source_semijoin_match_expression: bool = false,
    update_joined_source_exists_semijoin: bool = false,
    delete_joined_source_exists_semijoin: bool = false,
    update_joined_source_exists_match_expression: bool = false,
    delete_joined_source_exists_match_expression: bool = false,
    update_joined_source_row_value_semijoin: bool = false,
    delete_joined_source_row_value_semijoin: bool = false,
    update_joined_source_modulo_expression: bool = false,
    update_joined_source_regexp_expression: bool = false,
    delete_joined_source_regexp_expression: bool = false,
    update_joined_source_array_expression: bool = false,
    delete_joined_source_array_expression: bool = false,
    update_joined_source_json_expression: bool = false,
    delete_joined_source_json_expression: bool = false,
    update_joined_source_row_assignment: bool = false,
    update_joined_source_row_assignment_default: bool = false,
    update_joined_source_row_assignment_constructor: bool = false,
    update_source_patch_expression: bool = false,
    update_source_increment_expression: bool = false,
    update_source_modulo_expression: bool = false,
    update_source_regexp_replace_expression: bool = false,
    update_source_regexp_match_expression: bool = false,
    update_source_regexp_count_expression: bool = false,
    update_source_regexp_instr_expression: bool = false,
    update_source_regexp_substr_expression: bool = false,
    update_source_row_assignment: bool = false,
    update_source_row_assignment_default: bool = false,
    update_source_row_assignment_constructor: bool = false,
    schema_default_primary_named_conflict_target: bool = false,
    schema_custom_primary_named_conflict_target: bool = false,
    schema_unique_conflict_target: bool = false,
    schema_partial_unique_conflict_target: bool = false,
    schema_expression_unique_conflict_target: bool = false,
    schema_mixed_expression_unique_conflict_target: bool = false,
    schema_nulls_not_distinct_unique: bool = false,
    schema_temporal_numrange_insert: bool = false,
    schema_temporal_daterange_insert: bool = false,
    schema_temporal_open_daterange_insert: bool = false,
    schema_temporal_lower_open_daterange_insert: bool = false,
    schema_temporal_numrange_constructor_insert: bool = false,
    schema_temporal_daterange_constructor_insert: bool = false,
    schema_temporal_inclusive_daterange_constructor_insert: bool = false,
    schema_temporal_inclusive_daterange_literal_insert: bool = false,
    schema_temporal_lower_exclusive_daterange_constructor_insert: bool = false,
    schema_temporal_lower_exclusive_daterange_literal_insert: bool = false,
    schema_temporal_tsrange_insert: bool = false,
    schema_temporal_tsrange_constructor_insert: bool = false,
    schema_temporal_tstzrange_insert: bool = false,
    schema_temporal_tstzrange_constructor_insert: bool = false,
    schema_temporal_range_bound_query: bool = false,
    schema_temporal_range_contains_query: bool = false,
    schema_temporal_range_overlap_query: bool = false,
    schema_temporal_inclusive_daterange_overlap_query: bool = false,
    schema_temporal_unique_conflict_upsert: bool = false,
    schema_temporal_fk_ddl: bool = false,
    schema_temporal_portion_update: bool = false,
    schema_temporal_portion_delete: bool = false,
    schema_temporal_range_column_portion_update: bool = false,
    schema_temporal_range_column_portion_delete: bool = false,
    unsupported_ddl_system_time_temporal_table: bool = false,
    unsupported_duplicate_row_batch_target: bool = false,
    unsupported_duplicate_conflict_update_target: bool = false,
    unsupported_invalid_expression_conflict_target: bool = false,
    unsupported_invalid_named_conflict_target: bool = false,
    unsupported_unvalidated_unique_conflict_target: bool = false,
    to_jsonb_value_wrapper: bool = false,
    to_jsonb_dynamic_expression: bool = false,
    update_source_json_set_expression: bool = false,
    update_joined_source_json_set_expression: bool = false,
    query_substring_expression: bool = false,
    query_overlay_expression: bool = false,
    query_translate_expression: bool = false,
    query_split_part_expression: bool = false,
    query_strpos_expression: bool = false,
    query_left_right_expression: bool = false,
    query_trim_variant_expression: bool = false,
    query_regexp_replace_expression: bool = false,
    query_regexp_substr_expression: bool = false,
    query_regexp_match_expression: bool = false,
    query_regexp_count_expression: bool = false,
    query_regexp_instr_expression: bool = false,
    query_pad_expression: bool = false,
    query_repeat_expression: bool = false,
    query_reverse_expression: bool = false,
    query_initcap_expression: bool = false,
    query_text_length_expression: bool = false,
    query_bit_length_expression: bool = false,
    query_md5_expression: bool = false,
    query_concat_ws_expression: bool = false,
    query_nullif_expression: bool = false,
    query_extremum_expression: bool = false,
    query_nullable_pagination: bool = false,
    query_json_build_object_expression: bool = false,
    query_to_jsonb_expression: bool = false,
    query_convert_from_jsonb_expression: bool = false,
    query_cardinality_expression: bool = false,
    query_array_position_expression: bool = false,
    query_array_positions_expression: bool = false,
    query_array_element_transform_expression: bool = false,
    query_array_to_string_expression: bool = false,
    query_string_to_array_expression: bool = false,
    query_starts_with_expression: bool = false,
    query_ends_with_expression: bool = false,
    query_ascii_chr_expression: bool = false,
    query_modulo_expression: bool = false,
    aggregate_modulo_expression: bool = false,
    aggregate_octet_length_expression: bool = false,
    aggregate_bit_length_expression: bool = false,
    aggregate_scalar_minmax: bool = false,
    aggregate_regexp_numeric_expression: bool = false,
    aggregate_regexp_text_expression: bool = false,
    query_date_trunc_expression: bool = false,
    query_date_bin_expression: bool = false,
    query_typed_datetime_literal_expression: bool = false,
    query_date_part_expression: bool = false,
    query_date_part_epoch_expression: bool = false,
    conflict_date_bin_update: bool = false,
    conflict_typed_datetime_literal_update: bool = false,
    query_nested_case_fold_text_expression: bool = false,
    conflict_nested_text_expression_update: bool = false,
    ddl_create_table: bool = false,
    ddl_inline_named_column_constraints: bool = false,
    ddl_temporal_table: bool = false,
    ddl_replace_table: bool = false,
    ddl_create_index: bool = false,
    ddl_create_covering_index: bool = false,
    ddl_drop_index: bool = false,
    ddl_drop_table: bool = false,
    ddl_drop_table_cascade: bool = false,
    ddl_alter_table: bool = false,
    ddl_add_column_default_rewrite: bool = false,
    ddl_create_update_policy: bool = false,
    ddl_drop_update_policy: bool = false,
    ddl_add_unvalidated_unique: bool = false,
    ddl_add_unvalidated_fk: bool = false,
    ddl_add_unvalidated_check: bool = false,
    ddl_validate_constraint: bool = false,
    ddl_drop_constraint: bool = false,
    ddl_drop_column: bool = false,
    ddl_alter_column_default: bool = false,
    ddl_drop_column_default: bool = false,
    ddl_alter_column_not_null: bool = false,
    ddl_drop_column_not_null: bool = false,
    ddl_alter_column_type: bool = false,
    ddl_rename_column: bool = false,
    ddl_rename_constraint: bool = false,
    adapter_noop_transaction: bool = false,
    adapter_noop_transaction_commit: bool = false,
    adapter_noop_transaction_rollback: bool = false,
    adapter_noop_session: bool = false,
    adapter_noop_session_probe: bool = false,
    adapter_noop_schema_namespace: bool = false,
    adapter_noop_extension: bool = false,
    session_set_search_path: bool = false,
    session_reset_search_path: bool = false,
    session_show_search_path: bool = false,
    session_discard: bool = false,
    query_distinct_on: bool = false,
    query_cte_chain: bool = false,
    query_cte_structured_access: bool = false,
    query_cte_expression_access: bool = false,
    query_set_operation_order_limit: bool = false,
    read_set_operation_order_limit: bool = false,
    set_operation_fetch_tail: bool = false,
    set_operation_null_pagination_tail: bool = false,
    cte_set_operation_tail: bool = false,
    set_operation_numeric_range_disjoint: bool = false,
    set_operation_expression_numeric_range_disjoint: bool = false,
    aggregate_offset: bool = false,
    aggregate_input_expression: bool = false,
    aggregate_percentile_cont: bool = false,
    aggregate_percentile_disc: bool = false,
    aggregate_group_expression: bool = false,
    aggregate_group_expression_alias: bool = false,
    aggregate_having_expression: bool = false,
    aggregate_having_any: bool = false,
    aggregate_boolean_having_predicate: bool = false,
    aggregate_boolean_is_not_having: bool = false,
    aggregate_filter_expression: bool = false,
    aggregate_computed_pattern_filter: bool = false,
    aggregate_filter_groups: bool = false,
    aggregate_boolean_is_not_filter: bool = false,
    aggregate_boolean_unknown_filter: bool = false,
    aggregate_distinct_json_array_expression: bool = false,
    aggregate_distinct_group_projection: bool = false,
    aggregate_cte_expression_access: bool = false,
    join_structured_side_access: bool = false,
    join_on_side_predicate: bool = false,
    join_on_preserved_side_predicate: bool = false,
    join_on_computed_predicate: bool = false,
    join_computed_pattern_side_filter: bool = false,
    join_expression_order: bool = false,
    join_offset: bool = false,
    lateral_structured_side_access: bool = false,
    lateral_computed_pattern_side_filter: bool = false,
    lateral_subquery_match_expression: bool = false,
    lateral_subquery_match_expression_or: bool = false,
    lateral_subquery_function_match_expression_or: bool = false,
    lateral_subquery_match_expression_not: bool = false,
    lateral_subquery_match_expression_array: bool = false,
    lateral_expression_order: bool = false,
    lateral_right_offset: bool = false,
    window_rich_functions: bool = false,
    window_source_membership: bool = false,
    window_mixed_order: bool = false,
    window_expression_order: bool = false,
    window_boolean_aggregate_functions: bool = false,
    window_cte: bool = false,
    window_cte_expression_access: bool = false,
    window_offset: bool = false,
    window_frame_signature: bool = false,
    window_aggregate_filter: bool = false,
    window_computed_pattern_filter: bool = false,
    window_scalar_minmax: bool = false,
    window_modulo_expression: bool = false,
    joined_source_computed_pattern_filter: bool = false,
    parameterized_query: bool = false,
    parameterized_aggregate: bool = false,
    parameterized_join: bool = false,
    parameterized_lateral: bool = false,
    parameterized_window: bool = false,
    parameterized_insert: bool = false,
    parameterized_update: bool = false,
    parameterized_delete: bool = false,
    parameterized_update_source: bool = false,
    parameterized_delete_source: bool = false,
    parameterized_update_joined_source: bool = false,
    parameterized_delete_joined_source: bool = false,

    pub fn observe(self: *@This(), alloc: std.mem.Allocator, entry: AppParityCorpusEntry) !void {
        const uses_cte_stream = sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") or sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=");
        const uses_returning_all = sql_adapter.planHasNonZeroToken(entry.plan, ":returning_all=");
        const uses_conflict_where = sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_where=");
        const uses_insert_conflict = entry.family == .insert and std.mem.indexOf(u8, entry.sql, "ON CONFLICT") != null;
        const uses_multi_row_insert = entry.family == .insert and std.mem.indexOf(u8, entry.sql, "), (") != null;
        const uses_computed_pattern = appParitySqlHasComputedPattern(entry.sql);
        const is_update_joined_source = entry.family == .update_joined_source;
        const is_delete_joined_source = entry.family == .delete_joined_source;
        const is_joined_source = is_update_joined_source or is_delete_joined_source;
        if (entry.params.len > 0) {
            switch (entry.family) {
                .query => self.parameterized_query = true,
                .aggregate => self.parameterized_aggregate = true,
                .join => self.parameterized_join = true,
                .lateral => self.parameterized_lateral = true,
                .window => self.parameterized_window = true,
                .insert => self.parameterized_insert = true,
                .update => self.parameterized_update = true,
                .delete => self.parameterized_delete = true,
                .update_source => self.parameterized_update_source = true,
                .delete_source => self.parameterized_delete_source = true,
                .update_joined_source => self.parameterized_update_joined_source = true,
                .delete_joined_source => self.parameterized_delete_joined_source = true,
                else => {},
            }
        }
        self.to_jsonb_value_wrapper = self.to_jsonb_value_wrapper or std.mem.indexOf(u8, entry.sql, "to_jsonb(") != null;
        self.to_jsonb_dynamic_expression = self.to_jsonb_dynamic_expression or
            std.mem.indexOf(u8, entry.sql, "to_jsonb(lower(") != null or
            std.mem.indexOf(u8, entry.sql, "to_jsonb(excluded.") != null;
        self.update_source_json_set_expression = self.update_source_json_set_expression or
            (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":json_set_expr="));
        self.update_joined_source_json_set_expression = self.update_joined_source_json_set_expression or
            (entry.family == .update_joined_source and sql_adapter.planHasNonZeroToken(entry.plan, ":json_set_expr="));
        self.point_update_jsonb = self.point_update_jsonb or (entry.family == .update and std.mem.indexOf(u8, entry.sql, "jsonb_") != null);
        self.point_update_jsonb_concat = self.point_update_jsonb_concat or (entry.family == .update and
            std.mem.indexOf(u8, entry.sql, "metadata ||") != null and
            std.mem.indexOf(u8, entry.sql, "::jsonb") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.point_update_array = self.point_update_array or (entry.family == .update and std.mem.indexOf(u8, entry.sql, "array_") != null);
        self.write_plan_insert_op_set = self.write_plan_insert_op_set or (entry.family == .insert and sql_adapter.planHasNonZeroToken(entry.plan, ":op_set="));
        self.write_plan_insert_op_inc = self.write_plan_insert_op_inc or (entry.family == .insert and sql_adapter.planHasNonZeroToken(entry.plan, ":op_inc="));
        self.write_plan_update_op_set = self.write_plan_update_op_set or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_set="));
        self.write_plan_update_op_push = self.write_plan_update_op_push or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_push="));
        self.write_plan_update_op_pull = self.write_plan_update_op_pull or (entry.family == .update and sql_adapter.planHasNonZeroToken(entry.plan, ":op_pull="));
        self.point_update_uuid_generation = self.point_update_uuid_generation or (entry.family == .update and std.mem.indexOf(u8, entry.sql, "gen_random_uuid()") != null);
        self.point_update_patch_expression = self.point_update_patch_expression or
            (entry.family == .update and std.mem.eql(u8, entry.name, "point update expression assignment"));
        self.update_source_claim_skip_locked = self.update_source_claim_skip_locked or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "skip_locked", "no_key_update_skip_locked" }));
        self.update_source_claim_nowait = self.update_source_claim_nowait or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "nowait", "no_key_update_nowait" }));
        self.update_source_claim_no_key_update = self.update_source_claim_no_key_update or (entry.family == .update_source and
            sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "no_key_update", "no_key_update_nowait", "no_key_update_skip_locked" }));
        self.update_source_pagination = self.update_source_pagination or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":source_offset="));
        self.update_source_nullable_pagination = self.update_source_nullable_pagination or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "LIMIT NULL") != null and
            std.mem.indexOf(u8, entry.sql, "OFFSET NULL") != null and
            sql_adapter.planHasExactStringToken(entry.plan, ":source_limit=", "-1") and
            sql_adapter.planTokenAbsent(entry.plan, ":source_offset="));
        self.update_source_returning_expression = self.update_source_returning_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.schema_temporal_numrange_insert = self.schema_temporal_numrange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::numrange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "price_intervals") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_daterange_insert = self.schema_temporal_daterange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::daterange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            std.mem.indexOf(u8, entry.sql, ",)'") == null and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_open_daterange_insert = self.schema_temporal_open_daterange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::daterange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            std.mem.indexOf(u8, entry.sql, ",)'") != null and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_open_daterange_insert = self.schema_temporal_lower_open_daterange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::daterange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            std.mem.indexOf(u8, entry.sql, "'(,") != null and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_numrange_constructor_insert = self.schema_temporal_numrange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "numrange(") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "price_intervals") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_daterange_constructor_insert = self.schema_temporal_daterange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "daterange(") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_inclusive_daterange_constructor_insert = self.schema_temporal_inclusive_daterange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "daterange(") != null and
            std.mem.indexOf(u8, entry.sql, "'[]'") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_inclusive_daterange_literal_insert = self.schema_temporal_inclusive_daterange_literal_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::daterange") != null and
            std.mem.indexOf(u8, entry.sql, "2025-02-01]'") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_exclusive_daterange_constructor_insert = self.schema_temporal_lower_exclusive_daterange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "daterange(") != null and
            std.mem.indexOf(u8, entry.sql, "'(]'") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_lower_exclusive_daterange_literal_insert = self.schema_temporal_lower_exclusive_daterange_literal_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::daterange") != null and
            std.mem.indexOf(u8, entry.sql, "(2025-01-01,") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "products") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tsrange_insert = self.schema_temporal_tsrange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::tsrange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "local_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tsrange_constructor_insert = self.schema_temporal_tsrange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "tsrange(") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "local_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tstzrange_insert = self.schema_temporal_tstzrange_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "::tstzrange") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "published_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_tstzrange_constructor_insert = self.schema_temporal_tstzrange_constructor_insert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "tstzrange(") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "published_prices") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_bound_query = self.schema_temporal_range_bound_query or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "lower(") != null and
            std.mem.indexOf(u8, entry.sql, "upper(") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.schema_temporal_range_contains_query = self.schema_temporal_range_contains_query or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, " @> ") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_range_overlap_query = self.schema_temporal_range_overlap_query or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, " && ") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "price_intervals") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_inclusive_daterange_overlap_query = self.schema_temporal_inclusive_daterange_overlap_query or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, " && ") != null and
            std.mem.indexOf(u8, entry.sql, "daterange(") != null and
            std.mem.indexOf(u8, entry.sql, "'[]'") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "query:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.schema_temporal_unique_conflict_upsert = self.schema_temporal_unique_conflict_upsert or (entry.family == .insert and
            std.mem.indexOf(u8, entry.sql, "ON CONFLICT ON CONSTRAINT prices_sku_time_key") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "insert:table=", "prices") and
            sql_adapter.planHasExactUsizeToken(entry.plan, ":transforms=", 1) and
            entry.apply_setup_sql.len > 0 and
            entry.resolver_row_json.len > 0);
        self.query_set_operation_order_limit = self.query_set_operation_order_limit or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, " UNION ") != null and
            std.mem.indexOf(u8, entry.sql, " ORDER BY ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order=") and
            sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "5"));
        self.read_set_operation_order_limit = self.read_set_operation_order_limit or (entry.family == .read and
            std.mem.indexOf(u8, entry.sql, " INTERSECT ") != null and
            std.mem.indexOf(u8, entry.sql, " ORDER BY ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order=") and
            sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "5"));
        self.set_operation_fetch_tail = self.set_operation_fetch_tail or
            ((entry.family == .query or entry.family == .read) and
                std.mem.indexOf(u8, entry.sql, " UNION ALL ") != null and
                std.mem.indexOf(u8, entry.sql, " FETCH FIRST ROW ONLY") != null and
                sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "1"));
        self.set_operation_null_pagination_tail = self.set_operation_null_pagination_tail or
            ((entry.family == .query or entry.family == .read) and
                std.mem.indexOf(u8, entry.sql, " UNION ALL ") != null and
                std.mem.indexOf(u8, entry.sql, " LIMIT NULL OFFSET NULL") != null and
                sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "none") and
                sql_adapter.planTokenAbsent(entry.plan, ":offset="));
        self.cte_set_operation_tail = self.cte_set_operation_tail or
            ((entry.family == .query or entry.family == .read) and
                std.mem.indexOf(u8, entry.sql, "WITH scoped AS") != null and
                std.mem.indexOf(u8, entry.sql, " UNION ") != null and
                sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.set_operation_numeric_range_disjoint = self.set_operation_numeric_range_disjoint or
            ((entry.family == .query or entry.family == .read) and
                std.mem.indexOf(u8, entry.sql, " UNION ALL ") != null and
                (std.mem.indexOf(u8, entry.sql, "amount < 5") != null or std.mem.indexOf(u8, entry.sql, "amount <= 5") != null) and
                (std.mem.indexOf(u8, entry.sql, "amount >= 5") != null or std.mem.indexOf(u8, entry.sql, "amount > 5") != null) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":or="));
        self.set_operation_expression_numeric_range_disjoint = self.set_operation_expression_numeric_range_disjoint or
            ((entry.family == .query or entry.family == .read) and
                std.mem.indexOf(u8, entry.sql, " UNION ALL ") != null and
                std.mem.indexOf(u8, entry.sql, "amount + quantity") != null and
                (std.mem.indexOf(u8, entry.sql, "< 5") != null or std.mem.indexOf(u8, entry.sql, "<= 5") != null) and
                (std.mem.indexOf(u8, entry.sql, ">= 5") != null or std.mem.indexOf(u8, entry.sql, "> 5") != null) and
                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or="));
        self.schema_temporal_fk_ddl = self.schema_temporal_fk_ddl or (entry.family == .ddl and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal_fk=") and
            std.mem.indexOf(u8, entry.sql, "PERIOD ") != null and
            std.mem.indexOf(u8, entry.sql, "FOREIGN KEY") != null);
        self.schema_nulls_not_distinct_unique = self.schema_nulls_not_distinct_unique or (entry.family == .ddl and
            std.mem.indexOf(u8, entry.sql, "UNIQUE ") != null and
            std.mem.indexOf(u8, entry.sql, "NULLS NOT DISTINCT") != null);
        self.schema_temporal_portion_update = self.schema_temporal_portion_update or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "FOR PORTION OF") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "update_source:table=", "prices") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_portion_delete = self.schema_temporal_portion_delete or (entry.family == .delete_source and
            std.mem.indexOf(u8, entry.sql, "FOR PORTION OF") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "delete_source:table=", "prices") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_column_portion_update = self.schema_temporal_range_column_portion_update or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "FOR PORTION OF valid_at") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "update_source:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.schema_temporal_range_column_portion_delete = self.schema_temporal_range_column_portion_delete or (entry.family == .delete_source and
            std.mem.indexOf(u8, entry.sql, "FOR PORTION OF valid_at") != null and
            sql_adapter.planHasExactStringToken(entry.plan, "delete_source:table=", "products") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":temporal=") and
            entry.apply_setup_sql.len > 0);
        self.update_source_row_assignment = self.update_source_row_assignment or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "SET (status, priority)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_row_assignment_default = self.update_source_row_assignment_default or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "SET (status, priority)") != null and
            std.mem.indexOf(u8, entry.sql, "DEFAULT") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_row_assignment_constructor = self.update_source_row_assignment_constructor or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "SET (status, priority)") != null and
            std.mem.indexOf(u8, entry.sql, " = ROW(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_source_boolean_is_not_predicate = self.update_source_boolean_is_not_predicate or (entry.family == .update_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_or=") and
            (std.mem.indexOf(u8, entry.sql, " IS NOT TRUE") != null or
                std.mem.indexOf(u8, entry.sql, " IS NOT FALSE") != null));
        self.delete_source_fetch_pagination = self.delete_source_fetch_pagination or (entry.family == .delete_source and
            std.mem.indexOf(u8, entry.sql, "FETCH") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_offset="));
        self.delete_source_nullable_pagination = self.delete_source_nullable_pagination or (entry.family == .delete_source and
            std.mem.indexOf(u8, entry.sql, "LIMIT NULL") != null and
            std.mem.indexOf(u8, entry.sql, "OFFSET NULL") != null and
            sql_adapter.planHasExactStringToken(entry.plan, ":source_limit=", "-1") and
            sql_adapter.planTokenAbsent(entry.plan, ":source_offset="));
        self.delete_source_boolean_unknown_predicate = self.delete_source_boolean_unknown_predicate or (entry.family == .delete_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_pred=") and
            (std.mem.indexOf(u8, entry.sql, " IS UNKNOWN") != null or
                std.mem.indexOf(u8, entry.sql, " IS NOT UNKNOWN") != null));
        self.delete_source_returning_expression = self.delete_source_returning_expression or (entry.family == .delete_source and sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.joined_source_ordered_pagination = self.joined_source_ordered_pagination or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":limit=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":offset="));
        self.joined_source_expression_predicate = self.joined_source_expression_predicate or (is_joined_source and
            (sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_or=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_not=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_array=")));
        self.joined_source_computed_pattern_filter = self.joined_source_computed_pattern_filter or (is_joined_source and
            uses_computed_pattern and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred="));
        self.joined_source_expression_group = self.joined_source_expression_group or (is_joined_source and
            (sql_adapter.planHasNonZeroToken(entry.plan, "_expr_or=") or
                sql_adapter.planHasNonZeroToken(entry.plan, "_expr_not=")));
        self.joined_source_expression_array = self.joined_source_expression_array or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_array="));
        self.joined_source_returning_expression = self.joined_source_returning_expression or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.joined_source_returning_source_field = self.joined_source_returning_source_field or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            std.mem.indexOf(u8, entry.sql, "RETURNING") != null and
            std.mem.indexOf(u8, entry.sql, "source.") != null);
        self.joined_source_returning_source_expression = self.joined_source_returning_source_expression or (is_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            std.mem.indexOf(u8, entry.sql, "RETURNING") != null and
            std.mem.indexOf(u8, entry.sql, "lower(source.") != null);
        self.update_joined_source_returning_source_expression = self.update_joined_source_returning_source_expression or (entry.family == .update_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            std.mem.indexOf(u8, entry.sql, "RETURNING") != null and
            std.mem.indexOf(u8, entry.sql, "lower(source.") != null);
        self.delete_joined_source_returning_source_expression = self.delete_joined_source_returning_source_expression or (entry.family == .delete_joined_source and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=") and
            std.mem.indexOf(u8, entry.sql, "RETURNING") != null and
            std.mem.indexOf(u8, entry.sql, "lower(source.") != null);
        self.update_joined_source_non_primary_semijoin = self.update_joined_source_non_primary_semijoin or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "IN (SELECT organization_id FROM archived_records)") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 1));
        self.delete_joined_source_non_primary_semijoin = self.delete_joined_source_non_primary_semijoin or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "IN (SELECT organization_id FROM archived_records)") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 1));
        self.update_joined_source_correlated_semijoin = self.update_joined_source_correlated_semijoin or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "archived_records.status = usage_records.status") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.delete_joined_source_correlated_semijoin = self.delete_joined_source_correlated_semijoin or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "archived_records.status = usage_records.status") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.update_joined_source_correlated_filtered_semijoin = self.update_joined_source_correlated_filtered_semijoin or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "archived_records.organization_id = 'o1'") != null and
            std.mem.indexOf(u8, entry.sql, "archived_records.status = usage_records.status") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 2));
        self.delete_joined_source_correlated_filtered_semijoin = self.delete_joined_source_correlated_filtered_semijoin or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "archived_records.organization_id = 'o1'") != null and
            std.mem.indexOf(u8, entry.sql, "archived_records.status = usage_records.status") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 2));
        self.update_joined_source_semijoin_match_expression = self.update_joined_source_semijoin_match_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "lower(archived_records.status) = lower(usage_records.status)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.delete_joined_source_semijoin_match_expression = self.delete_joined_source_semijoin_match_expression or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "lower(archived_records.status) = lower(usage_records.status)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.update_joined_source_exists_semijoin = self.update_joined_source_exists_semijoin or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE EXISTS") != null and
            std.mem.indexOf(u8, entry.sql, "archived_records.organization_id = usage_records.id") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 1));
        self.delete_joined_source_exists_semijoin = self.delete_joined_source_exists_semijoin or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE EXISTS") != null and
            std.mem.indexOf(u8, entry.sql, "archived_records.organization_id = usage_records.id") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 1, 1));
        self.update_joined_source_exists_match_expression = self.update_joined_source_exists_match_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE EXISTS") != null and
            std.mem.indexOf(u8, entry.sql, "lower(archived_records.status) = lower(usage_records.status)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.delete_joined_source_exists_match_expression = self.delete_joined_source_exists_match_expression or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE EXISTS") != null and
            std.mem.indexOf(u8, entry.sql, "lower(archived_records.status) = lower(usage_records.status)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
        self.update_joined_source_row_value_semijoin = self.update_joined_source_row_value_semijoin or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE (id, status) IN") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.delete_joined_source_row_value_semijoin = self.delete_joined_source_row_value_semijoin or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "WHERE (id, status) IN") != null and
            sql_adapter.joinedSourcePlanHasCounts(entry.plan, 0, 2));
        self.update_joined_source_modulo_expression = self.update_joined_source_modulo_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "MOD(source.quantity + usage_records.quantity, 7)") != null and
            std.mem.indexOf(u8, entry.sql, "quantity % 2") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_regexp_expression = self.update_joined_source_regexp_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "regexp_like(source.status") != null and
            std.mem.indexOf(u8, entry.sql, "regexp_substr(source.status") != null and
            std.mem.indexOf(u8, entry.sql, "regexp_count(source.status") != null and
            std.mem.indexOf(u8, entry.sql, "regexp_instr(source.status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_regexp_expression = self.delete_joined_source_regexp_expression or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "regexp_like(source.status") != null and
            std.mem.indexOf(u8, entry.sql, "regexp_substr(source.status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_array_expression = self.update_joined_source_array_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "array_append(source.tags") != null and
            std.mem.indexOf(u8, entry.sql, "array_position(source.tags") != null and
            std.mem.indexOf(u8, entry.sql, "array_to_string(source.tags") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_array_expression = self.delete_joined_source_array_expression or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "array_position(source.tags") != null and
            std.mem.indexOf(u8, entry.sql, "array_to_string(source.tags") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_json_expression = self.update_joined_source_json_expression or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "jsonb_build_object('status', source.status") != null and
            std.mem.indexOf(u8, entry.sql, "to_jsonb(source.tags") != null and
            std.mem.indexOf(u8, entry.sql, "jsonb_extract_path_text(source.metadata") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.delete_joined_source_json_expression = self.delete_joined_source_json_expression or (is_delete_joined_source and
            std.mem.indexOf(u8, entry.sql, "jsonb_build_object('source'") != null and
            std.mem.indexOf(u8, entry.sql, "to_jsonb(source.tags") != null and
            std.mem.indexOf(u8, entry.sql, "jsonb_extract_path_text(source.metadata") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, "_expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_joined_source_row_assignment = self.update_joined_source_row_assignment or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "SET (quantity, status)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_row_assignment_default = self.update_joined_source_row_assignment_default or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "SET (quantity, status)") != null and
            std.mem.indexOf(u8, entry.sql, "DEFAULT") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_row_assignment_constructor = self.update_joined_source_row_assignment_constructor or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "SET (quantity, status)") != null and
            std.mem.indexOf(u8, entry.sql, " = ROW(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_assignments=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":ops="));
        self.update_joined_source_boolean_expression_update = self.update_joined_source_boolean_expression_update or (is_update_joined_source and
            std.mem.indexOf(u8, entry.sql, "SET enabled = usage_records.enabled OR source.enabled") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_patch_expression = self.update_source_patch_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_boolean_expression_update = self.update_source_boolean_expression_update or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "SET enabled = enabled OR false") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_increment_expression = self.update_source_increment_expression or (entry.family == .update_source and sql_adapter.planHasNonZeroToken(entry.plan, ":increment_expr="));
        self.update_source_modulo_expression = self.update_source_modulo_expression or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "MOD(priority + 9, 7)") != null and
            std.mem.indexOf(u8, entry.sql, "priority % 2") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.update_source_regexp_replace_expression = self.update_source_regexp_replace_expression or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "regexp_replace(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_match_expression = self.update_source_regexp_match_expression or (entry.family == .update_source and
            (std.mem.indexOf(u8, entry.sql, "regexp_like(status") != null or
                std.mem.indexOf(u8, entry.sql, "regexp_match(status") != null) and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_count_expression = self.update_source_regexp_count_expression or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "regexp_count(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_instr_expression = self.update_source_regexp_instr_expression or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "regexp_instr(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.update_source_regexp_substr_expression = self.update_source_regexp_substr_expression or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, "regexp_substr(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":patch_expr="));
        self.catalog_setup_sql = self.catalog_setup_sql or entry.apply_setup_sql.len > 0;
        if (entry.applied_plan.len > 0) {
            self.applied_catalog_plan = true;
            self.applied_catalog_rebuild = self.applied_catalog_rebuild or sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", true);
            self.applied_catalog_validation = self.applied_catalog_validation or sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", true);
            self.applied_catalog_rewrite = self.applied_catalog_rewrite or sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", true);
        }
        switch (entry.family) {
            .ddl => self.ddl = true,
            .query => {
                self.query = true;
                self.cte_query = self.cte_query or uses_cte_stream;
                self.query_distinct_on = self.query_distinct_on or sql_adapter.planHasNonZeroToken(entry.plan, ":distinct_on=");
                self.query_cte_chain = self.query_cte_chain or sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 2);
                self.query_cte_structured_access = self.query_cte_structured_access or
                    sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_");
                self.query_cte_expression_access = self.query_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_array="))));
                self.query_computed_pattern_predicate = self.query_computed_pattern_predicate or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=");
            },
            .aggregate => {
                self.aggregate = true;
                self.cte_aggregate = self.cte_aggregate or uses_cte_stream;
                self.aggregate_offset = self.aggregate_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
                self.aggregate_input_expression = self.aggregate_input_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr=");
                self.aggregate_modulo_expression = self.aggregate_modulo_expression or (std.mem.indexOf(u8, entry.sql, "SUM(quantity % 7)") != null and
                    std.mem.indexOf(u8, entry.sql, "SUM(MOD(amount + quantity") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_octet_length_expression = self.aggregate_octet_length_expression or (std.mem.indexOf(u8, entry.sql, "SUM(octet_length(status))") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_bit_length_expression = self.aggregate_bit_length_expression or (std.mem.indexOf(u8, entry.sql, "SUM(bit_length(status))") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_scalar_minmax = self.aggregate_scalar_minmax or (std.mem.indexOf(u8, entry.sql, "MIN(status)") != null and
                    std.mem.indexOf(u8, entry.sql, "MAX(lower(status))") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_regexp_numeric_expression = self.aggregate_regexp_numeric_expression or (std.mem.indexOf(u8, entry.sql, "SUM(regexp_count(status,") != null and
                    std.mem.indexOf(u8, entry.sql, "SUM(regexp_instr(status,") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_regexp_text_expression = self.aggregate_regexp_text_expression or (std.mem.indexOf(u8, entry.sql, "COUNT(DISTINCT regexp_substr(status,") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr="));
                self.aggregate_percentile_cont = self.aggregate_percentile_cont or
                    std.mem.indexOf(u8, entry.sql, "percentile_cont") != null;
                self.aggregate_percentile_disc = self.aggregate_percentile_disc or
                    std.mem.indexOf(u8, entry.sql, "percentile_disc") != null;
                self.aggregate_group_expression = self.aggregate_group_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":group_expr=");
                self.aggregate_group_expression_alias = self.aggregate_group_expression_alias or (sql_adapter.planHasNonZeroToken(entry.plan, ":group_expr=") and
                    std.mem.indexOf(u8, entry.sql, "GROUP BY status_key") != null);
                self.aggregate_having_expression = self.aggregate_having_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":having_expr=");
                self.aggregate_having_any = self.aggregate_having_any or sql_adapter.planHasNonZeroToken(entry.plan, ":having_any=");
                self.aggregate_boolean_having_predicate = self.aggregate_boolean_having_predicate or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":having=") and
                        (std.mem.indexOf(u8, entry.sql, "HAVING enabled IS TRUE") != null or
                            std.mem.indexOf(u8, entry.sql, "HAVING enabled IS FALSE") != null or
                            std.mem.indexOf(u8, entry.sql, "HAVING enabled IS UNKNOWN") != null or
                            std.mem.indexOf(u8, entry.sql, "HAVING enabled IS NOT UNKNOWN") != null);
                self.aggregate_boolean_is_not_having = self.aggregate_boolean_is_not_having or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":having_any=") and
                        (std.mem.indexOf(u8, entry.sql, "HAVING enabled IS NOT TRUE") != null or
                            std.mem.indexOf(u8, entry.sql, "HAVING enabled IS NOT FALSE") != null);
                self.aggregate_filter_expression = self.aggregate_filter_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":filter_expr=");
                self.aggregate_computed_pattern_filter = self.aggregate_computed_pattern_filter or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":filter_expr=");
                self.aggregate_filter_groups = self.aggregate_filter_groups or sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=");
                self.aggregate_boolean_is_not_filter = self.aggregate_boolean_is_not_filter or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=") and
                        (std.mem.indexOf(u8, entry.sql, "FILTER (WHERE enabled IS NOT TRUE") != null or
                            std.mem.indexOf(u8, entry.sql, "FILTER (WHERE enabled IS NOT FALSE") != null);
                self.aggregate_boolean_unknown_filter = self.aggregate_boolean_unknown_filter or
                    (sql_adapter.planHasNonZeroToken(entry.plan, ":filter_groups=") or sql_adapter.planHasNonZeroToken(entry.plan, ":aggs=")) and
                        (std.mem.indexOf(u8, entry.sql, "FILTER (WHERE enabled IS UNKNOWN") != null or
                            std.mem.indexOf(u8, entry.sql, "FILTER (WHERE enabled IS NOT UNKNOWN") != null);
                self.aggregate_distinct_json_array_expression = self.aggregate_distinct_json_array_expression or
                    std.mem.indexOf(u8, entry.sql, "array_agg(DISTINCT metadata->'flags')") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":agg_expr=");
                self.aggregate_distinct_group_projection = self.aggregate_distinct_group_projection or
                    (sql_adapter.planHasNonZeroToken(entry.plan, ":group=") and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":aggs=", 0));
                self.aggregate_cte_expression_access = self.aggregate_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":ctes=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array="))));
            },
            .join => {
                self.join = true;
                self.join_structured_side_access = self.join_structured_side_access or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":left_json_contains=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":right_json_exists=");
                self.join_on_side_predicate = self.join_on_side_predicate or
                    std.mem.indexOf(u8, entry.sql, "ON o.customer_id = c.id AND c.kind = 'customer'") != null;
                self.join_on_preserved_side_predicate = self.join_on_preserved_side_predicate or
                    std.mem.indexOf(u8, entry.sql, "ON o.customer_id = c.id AND o.kind = 'order'") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":on_expr_pred=");
                self.join_on_computed_predicate = self.join_on_computed_predicate or
                    std.mem.indexOf(u8, entry.sql, "ON o.customer_id = c.id AND lower(o.kind) = lower(c.kind)") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":on_expr_pred=");
                self.join_computed_pattern_side_filter = self.join_computed_pattern_side_filter or
                    uses_computed_pattern and
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":left_expr_pred=") or
                            sql_adapter.planHasNonZeroToken(entry.plan, ":right_expr_pred="));
                self.join_expression_order = self.join_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.join_offset = self.join_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
            },
            .lateral => {
                self.lateral = true;
                self.lateral_structured_side_access = self.lateral_structured_side_access or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":left_json_contains=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":right_json_exists=");
                self.lateral_computed_pattern_side_filter = self.lateral_computed_pattern_side_filter or
                    uses_computed_pattern and
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":left_expr_pred=") or
                            sql_adapter.planHasNonZeroToken(entry.plan, ":right_expr_pred="));
                self.lateral_subquery_match_expression = self.lateral_subquery_match_expression or
                    (std.mem.indexOf(u8, entry.sql, "bal.amount + org.amount") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_pred="));
                self.lateral_subquery_match_expression_or = self.lateral_subquery_match_expression_or or
                    (std.mem.indexOf(u8, entry.sql, "bal.amount + org.amount < 100") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_or="));
                self.lateral_subquery_function_match_expression_or = self.lateral_subquery_function_match_expression_or or
                    (std.mem.indexOf(u8, entry.sql, "lower(bal.kind) = lower(org.kind)") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_or="));
                self.lateral_subquery_match_expression_not = self.lateral_subquery_match_expression_not or
                    (std.mem.indexOf(u8, entry.sql, "NOT (bal.amount + org.amount > 100)") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_not="));
                self.lateral_subquery_match_expression_array = self.lateral_subquery_match_expression_array or
                    (std.mem.indexOf(u8, entry.sql, "string_to_array(bal.status || ' ' || org.status") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":match_expr_array="));
                self.lateral_expression_order = self.lateral_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.lateral_right_offset = self.lateral_right_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":right_offset=");
            },
            .window => {
                self.window = true;
                self.cte_window = self.cte_window or uses_cte_stream;
                self.window_rich_functions = self.window_rich_functions or sql_adapter.planHasNonZeroToken(entry.plan, ":windows=");
                self.window_source_membership = self.window_source_membership or sql_adapter.planHasNonZeroToken(entry.plan, ":source_in=");
                self.window_mixed_order = self.window_mixed_order or
                    (std.mem.indexOf(u8, entry.sql, "AS amount_row_num") != null and
                        std.mem.indexOf(u8, entry.sql, "AS id_row_num") != null and
                        sql_adapter.planHasNonZeroToken(entry.plan, ":windows="));
                self.window_expression_order = self.window_expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=");
                self.window_modulo_expression = self.window_modulo_expression or (std.mem.indexOf(u8, entry.sql, "sum(amount % quantity)") != null and
                    std.mem.indexOf(u8, entry.sql, "avg(MOD(amount + quantity") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_expr="));
                self.window_scalar_minmax = self.window_scalar_minmax or (std.mem.indexOf(u8, entry.sql, "min(status) OVER") != null and
                    std.mem.indexOf(u8, entry.sql, "max(lower(status)) OVER") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_expr="));
                self.window_boolean_aggregate_functions = self.window_boolean_aggregate_functions or (std.mem.indexOf(u8, entry.sql, "bool_or(") != null and
                    std.mem.indexOf(u8, entry.sql, "bool_and(") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":windows=") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter=") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr="));
                self.window_cte = self.window_cte or uses_cte_stream;
                self.window_cte_expression_access = self.window_cte_expression_access or
                    (sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_") or
                        (sql_adapter.planHasNonZeroToken(entry.plan, ":source_cte=") and
                            (sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array="))));
                self.window_offset = self.window_offset or sql_adapter.planHasNonZeroToken(entry.plan, ":offset=");
                self.window_frame_signature = self.window_frame_signature or sql_adapter.planHasNonZeroToken(entry.plan, ":window_frame_sig=");
                self.window_aggregate_filter = self.window_aggregate_filter or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_access=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_groups=");
                self.window_computed_pattern_filter = self.window_computed_pattern_filter or
                    uses_computed_pattern and sql_adapter.planHasNonZeroToken(entry.plan, ":window_filter_expr=");
            },
            .explain => {
                self.explain = true;
                self.explain_write = self.explain_write or sql_adapter.explainPlanHasKind(entry.plan, "write");
                self.explain_options = self.explain_options or
                    sql_adapter.planHasStringToken(entry.plan, ":format=") or
                    sql_adapter.planUsizeTokenValue(entry.plan, ":verbose=") != null or
                    sql_adapter.planUsizeTokenValue(entry.plan, ":costs=") != null;
                self.explain_analyze = self.explain_analyze or sql_adapter.planHasExactBoolToken(entry.plan, ":analyze=", true);
            },
            .relation_population => {
                self.relation_population_select_into = self.relation_population_select_into or
                    sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "select_into");
                self.relation_population_create_table_as = self.relation_population_create_table_as or
                    sql_adapter.planHasExactStringToken(entry.plan, "relation_population:mode=", "create_table_as");
            },
            .insert => self.insert = true,
            .insert_source => self.insert_source = true,
            .update => {
                self.update = true;
                self.update_identity_rewrite = self.update_identity_rewrite or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":identity_rewrites=");
            },
            .delete => self.delete = true,
            .update_source => self.update_source = true,
            .delete_source => self.delete_source = true,
            .truncate_source => self.truncate_source = true,
            .update_joined_source => {
                self.update_joined_source = true;
                self.update_joined_source_cte_mutation = self.update_joined_source_cte_mutation or
                    (std.mem.startsWith(u8, entry.sql, "WITH ") and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 1));
            },
            .delete_joined_source => {
                self.delete_joined_source = true;
                self.delete_joined_source_cte_mutation = self.delete_joined_source_cte_mutation or
                    (std.mem.startsWith(u8, entry.sql, "WITH ") and
                        sql_adapter.planHasExactUsizeToken(entry.plan, ":ctes=", 1));
            },
            .merge_mutation => self.merge_mutation_typed_plan = self.merge_mutation_typed_plan or std.mem.startsWith(u8, entry.plan, "merge_mutation:"),
            .adapter_noop_ddl => self.adapter_noop_ddl = true,
            .unsupported => self.unsupported_query = true,
            .unsupported_read => self.unsupported_read = true,
            .unsupported_ddl => self.unsupported_ddl = true,
            .unsupported_write => self.unsupported_write = true,
            .unsupported_insert => self.unsupported_insert = true,
            .unsupported_update => self.unsupported_update = true,
            .unsupported_update_source => self.unsupported_update_source = true,
            .unsupported_delete => self.unsupported_delete = true,
            .unsupported_update_joined_source => self.unsupported_update_joined_source = true,
            .unsupported_delete_joined_source => self.unsupported_delete_joined_source = true,
            .unsupported_merge_mutation => self.unsupported_merge_mutation = true,
            .read => {
                const is_read_query = std.mem.startsWith(u8, entry.plan, "read:query:");
                const is_read_aggregate = std.mem.startsWith(u8, entry.plan, "read:aggregate:");
                const is_read_window = std.mem.startsWith(u8, entry.plan, "read:window:");
                const has_cte_expression =
                    sql_adapter.planHasNonZeroUsizeTokenNamePrefix(entry.plan, "cte0_expr_");
                const has_read_query_expression =
                    has_cte_expression or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_not=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":expr_array=");
                const has_read_source_expression =
                    has_cte_expression or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not=") or
                    sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_array=");
                self.read = true;
                self.read_query = self.read_query or is_read_query;
                self.read_aggregate = self.read_aggregate or is_read_aggregate;
                self.read_join = self.read_join or std.mem.startsWith(u8, entry.plan, "read:join:");
                self.read_lateral = self.read_lateral or std.mem.startsWith(u8, entry.plan, "read:lateral:");
                self.read_window = self.read_window or is_read_window;
                self.read_join_cross_table_source_schema_classifier = self.read_join_cross_table_source_schema_classifier or
                    (std.mem.startsWith(u8, entry.plan, "read:join:") and
                        entry.source_schema_json.len > 0 and
                        sql_adapter.planHasExactStringToken(entry.plan, ":right=", "customer_records"));
                self.read_lateral_cross_table_source_schema_classifier = self.read_lateral_cross_table_source_schema_classifier or
                    (std.mem.startsWith(u8, entry.plan, "read:lateral:") and
                        entry.source_schema_json.len > 0 and
                        sql_adapter.planHasExactStringToken(entry.plan, ":right=", "balance_records"));
                self.read_cte_query_expression = self.read_cte_query_expression or
                    (is_read_query and has_read_query_expression);
                self.read_cte_aggregate_expression = self.read_cte_aggregate_expression or
                    (is_read_aggregate and has_read_source_expression);
                self.read_cte_window_expression = self.read_cte_window_expression or
                    (is_read_window and has_read_source_expression);
            },
        }
        if (entry.family == .unsupported) {
            self.unsupported_query_recursive_cte_stream_plan = self.unsupported_query_recursive_cte_stream_plan or std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan");
            self.unsupported_query_set_operation_plan = self.unsupported_query_set_operation_plan or std.mem.eql(u8, entry.classification_reason, "set_operation_plan");
        } else if (entry.family == .unsupported_read) {
            self.unsupported_read_recursive_cte_stream_plan = self.unsupported_read_recursive_cte_stream_plan or std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan");
            self.unsupported_read_duplicate_output_name = self.unsupported_read_duplicate_output_name or std.mem.eql(u8, entry.classification_reason, "duplicate_output_name");
            self.unsupported_read_aggregate_duplicate_output_name = self.unsupported_read_aggregate_duplicate_output_name or
                std.mem.eql(u8, entry.classification_reason, "aggregate_duplicate_output_name");
            self.unsupported_read_set_operation_union = self.unsupported_read_set_operation_union or
                (std.mem.eql(u8, entry.classification_reason, "set_operation_plan") and
                    std.mem.indexOf(u8, entry.sql, " UNION ") != null);
            self.unsupported_read_set_operation_intersect = self.unsupported_read_set_operation_intersect or
                (std.mem.eql(u8, entry.classification_reason, "set_operation_plan") and
                    std.mem.indexOf(u8, entry.sql, " INTERSECT ") != null);
            self.unsupported_read_set_operation_except = self.unsupported_read_set_operation_except or
                (std.mem.eql(u8, entry.classification_reason, "set_operation_plan") and
                    std.mem.indexOf(u8, entry.sql, " EXCEPT ") != null);
            self.unsupported_read_ordered_set_aggregate_plan = self.unsupported_read_ordered_set_aggregate_plan or
                (std.mem.eql(u8, entry.classification_reason, "ordered_set_aggregate_plan") and
                    std.mem.indexOf(u8, entry.sql, "WITHIN GROUP") != null);
            self.unsupported_read_row_lock_target = self.unsupported_read_row_lock_target or
                (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                    std.mem.indexOf(u8, entry.sql, "FOR UPDATE OF archived_records") != null);
        } else if (entry.family == .merge_mutation) {
            self.merge_mutation_default_expressions = self.merge_mutation_default_expressions or
                (std.mem.indexOf(u8, entry.sql, "DEFAULT") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":matched_update_expr=") and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":not_matched_insert_expr="));
        } else if (entry.family == .unsupported_write) {
            self.unsupported_write_recursive_cte_insert = self.unsupported_write_recursive_cte_insert or
                (std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan") and
                    std.mem.startsWith(u8, entry.sql, "WITH RECURSIVE ") and
                    std.mem.indexOf(u8, entry.sql, " INSERT INTO ") != null);
            self.unsupported_write_recursive_cte_update = self.unsupported_write_recursive_cte_update or
                (std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan") and
                    std.mem.startsWith(u8, entry.sql, "WITH RECURSIVE ") and
                    std.mem.indexOf(u8, entry.sql, " UPDATE ") != null);
            self.unsupported_write_recursive_cte_delete = self.unsupported_write_recursive_cte_delete or
                (std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan") and
                    std.mem.startsWith(u8, entry.sql, "WITH RECURSIVE ") and
                    std.mem.indexOf(u8, entry.sql, " DELETE FROM ") != null);
            self.unsupported_write_recursive_cte_merge = self.unsupported_write_recursive_cte_merge or
                (std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan") and
                    std.mem.startsWith(u8, entry.sql, "WITH RECURSIVE ") and
                    std.mem.indexOf(u8, entry.sql, " MERGE INTO ") != null);
            self.unsupported_write_truncate_multi_table = self.unsupported_write_truncate_multi_table or
                (std.mem.eql(u8, entry.classification_reason, "multi_table_generation_barrier") and
                    std.mem.indexOf(u8, entry.sql, ", archived_records") != null);
            self.unsupported_write_truncate_cascade = self.unsupported_write_truncate_cascade or
                (std.mem.eql(u8, entry.classification_reason, "multi_table_generation_barrier") and
                    std.mem.indexOf(u8, entry.sql, " CASCADE") != null);
        } else if (entry.family == .truncate_source) {
            self.truncate_continue_identity = self.truncate_continue_identity or
                (std.mem.indexOf(u8, entry.sql, "CONTINUE IDENTITY") != null and
                    sql_adapter.planHasExactStringToken(entry.plan, "truncate_source:table=", "usage_records"));
            self.truncate_restart_identity = self.truncate_restart_identity or
                (std.mem.indexOf(u8, entry.sql, "RESTART IDENTITY") != null and
                    sql_adapter.planHasExactUsizeToken(entry.plan, ":restart_identity=", 1));
        } else if (entry.family == .unsupported_ddl) {
            self.unsupported_ddl_temporal_fk_update_action = self.unsupported_ddl_temporal_fk_update_action or
                (std.mem.eql(u8, entry.classification_reason, "temporal_fk_action") and
                    std.mem.indexOf(u8, entry.sql, " ON UPDATE ") != null);
            self.unsupported_ddl_prepare_recursive_cte_statement = self.unsupported_ddl_prepare_recursive_cte_statement or
                (std.mem.eql(u8, entry.classification_reason, "recursive_cte_stream_plan") and
                    std.mem.startsWith(u8, entry.sql, "PREPARE ") and
                    std.mem.indexOf(u8, entry.sql, " AS WITH RECURSIVE ") != null);
            self.unsupported_ddl_deferrable_unique_constraint = self.unsupported_ddl_deferrable_unique_constraint or
                (std.mem.eql(u8, entry.classification_reason, "deferrable_unique_constraint") and
                    std.mem.indexOf(u8, entry.sql, " UNIQUE ") != null and
                    std.mem.indexOf(u8, entry.sql, " DEFERRABLE") != null);
            self.unsupported_ddl_deferrable_primary_key = self.unsupported_ddl_deferrable_primary_key or
                (std.mem.eql(u8, entry.classification_reason, "deferrable_primary_key") and
                    std.mem.indexOf(u8, entry.sql, " PRIMARY KEY ") != null and
                    std.mem.indexOf(u8, entry.sql, " DEFERRABLE") != null);
            self.unsupported_ddl_transaction_scoped_search_path = self.unsupported_ddl_transaction_scoped_search_path or
                (std.mem.eql(u8, entry.classification_reason, "transaction_scoped_search_path") and
                    std.mem.indexOf(u8, entry.sql, "SET LOCAL search_path") != null);
            self.unsupported_ddl_system_time_temporal_table = self.unsupported_ddl_system_time_temporal_table or
                (std.mem.eql(u8, entry.classification_reason, "system_time_temporal_table") and
                    std.mem.indexOf(u8, entry.sql, "SYSTEM VERSIONING") != null);
        } else if (entry.family == .unsupported_update) {
            self.unsupported_update_non_unique_point_selector = self.unsupported_update_non_unique_point_selector or std.mem.eql(u8, entry.classification_reason, "non_unique_point_selector");
        } else if (entry.family == .unsupported_update_source) {
            self.unsupported_update_source_row_lock_target = self.unsupported_update_source_row_lock_target or
                (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                    std.mem.indexOf(u8, entry.sql, "FOR UPDATE OF archived_records") != null);
        } else if (entry.family == .unsupported_delete) {
            self.unsupported_delete_non_unique_point_selector = self.unsupported_delete_non_unique_point_selector or std.mem.eql(u8, entry.classification_reason, "non_unique_point_selector");
            self.unsupported_delete_multi_output_subquery_selector = self.unsupported_delete_multi_output_subquery_selector or std.mem.eql(u8, entry.classification_reason, "multi_output_subquery_delete_selector");
        } else if (entry.family == .unsupported_update_joined_source) {
            self.unsupported_update_joined_multi_output_subquery_selector = self.unsupported_update_joined_multi_output_subquery_selector or std.mem.eql(u8, entry.classification_reason, "multi_output_subquery_update_selector");
            self.unsupported_update_joined_source_row_lock_target = self.unsupported_update_joined_source_row_lock_target or
                (std.mem.eql(u8, entry.classification_reason, "row_lock_mode_plan") and
                    std.mem.indexOf(u8, entry.sql, "FOR UPDATE OF source") != null);
        } else if (entry.family == .unsupported_delete_joined_source) {
            self.unsupported_delete_joined_multi_output_subquery_selector = self.unsupported_delete_joined_multi_output_subquery_selector or std.mem.eql(u8, entry.classification_reason, "multi_output_subquery_delete_selector");
        } else if (entry.family == .unsupported_merge_mutation) {
            self.unsupported_merge_mutation_cte = self.unsupported_merge_mutation_cte or
                (std.mem.eql(u8, entry.classification_reason, "cte_mutation_source_plan") and
                    std.mem.startsWith(u8, entry.sql, "WITH ") and
                    std.mem.indexOf(u8, entry.sql, " MERGE ") != null);
        }
        if (entry.family == .ddl) {
            switch (entry.summary.ddl_tag orelse return error.TestUnexpectedResult) {
                .create_table => {
                    self.ddl_create_table = true;
                    self.ddl_inline_named_column_constraints = self.ddl_inline_named_column_constraints or
                        (std.mem.indexOf(u8, entry.sql, "CONSTRAINT inline_constraints_pk PRIMARY KEY") != null and
                            std.mem.indexOf(u8, entry.sql, "CONSTRAINT inline_constraints_tenant_fkey REFERENCES") != null and
                            std.mem.indexOf(u8, entry.sql, "CONSTRAINT inline_constraints_email_key UNIQUE") != null and
                            std.mem.indexOf(u8, entry.sql, "CONSTRAINT inline_constraints_amount_check CHECK") != null and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":pk=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":unique=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":fk=", 1) and
                            sql_adapter.planHasExactUsizeToken(entry.plan, ":checks=", 1) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_unique=", 0) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_fk=", 0) and
                            sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_check=", 0));
                    self.ddl_temporal_table = self.ddl_temporal_table or sql_adapter.planHasNonZeroToken(entry.plan, ":periods=");
                    self.ddl_temporal_fk_delete_set_null_action = self.ddl_temporal_fk_delete_set_null_action or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":temporal_fk=", 1) and
                            std.mem.indexOf(u8, entry.sql, " ON DELETE SET NULL") != null);
                    self.ddl_temporal_fk_delete_cascade_action = self.ddl_temporal_fk_delete_cascade_action or
                        (sql_adapter.planHasExactUsizeToken(entry.plan, ":temporal_fk=", 1) and
                            std.mem.indexOf(u8, entry.sql, " ON DELETE CASCADE") != null);
                    self.ddl_replace_table = self.ddl_replace_table or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                },
                .table_clone => self.ddl_table_clone = true,
                .create_view => self.ddl_view_create = true,
                .rename_view => self.ddl_view_rename = true,
                .drop_view => self.ddl_view_drop = true,
                .create_materialized_view => self.ddl_materialized_view_create = true,
                .refresh_materialized_view => self.ddl_materialized_view_refresh = true,
                .drop_materialized_view => self.ddl_materialized_view_drop = true,
                .relation_lifetime => {
                    self.ddl_relation_lifetime_temporary = self.ddl_relation_lifetime_temporary or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "temporary");
                    self.ddl_relation_lifetime_unlogged = self.ddl_relation_lifetime_unlogged or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "unlogged");
                },
                .create_enum_type => self.ddl_enum_type_create = true,
                .add_enum_value => self.ddl_enum_type_add_value = true,
                .drop_enum_type => self.ddl_enum_type_drop = true,
                .create_domain => self.ddl_domain_create = true,
                .alter_domain => self.ddl_domain_alter = true,
                .drop_domain => self.ddl_domain_drop = true,
                .create_sequence => {
                    self.ddl_sequence_create = true;
                    self.ddl_sequence_create_typed_owned = self.ddl_sequence_create_typed_owned or
                        (std.mem.indexOf(u8, entry.sql, " AS bigint ") != null and
                            std.mem.indexOf(u8, entry.sql, " OWNED BY ") != null);
                },
                .alter_sequence => {
                    self.ddl_sequence_alter = true;
                    self.ddl_sequence_alter_typed_owned = self.ddl_sequence_alter_typed_owned or
                        (std.mem.indexOf(u8, entry.sql, " AS integer ") != null and
                            std.mem.indexOf(u8, entry.sql, " OWNED BY NONE") != null);
                },
                .drop_sequence => self.ddl_sequence_drop = true,
                .identity_allocator => {
                    self.ddl_identity_allocator_serial = self.ddl_identity_allocator_serial or sql_adapter.planHasAnyExactStringToken(entry.plan, ":kind=", &.{ "serial", "bigserial" });
                    self.ddl_identity_allocator_generated = self.ddl_identity_allocator_generated or sql_adapter.planHasAnyExactStringToken(entry.plan, ":kind=", &.{ "generated_by_default", "generated_always" });
                    self.ddl_identity_allocator_generated_options = self.ddl_identity_allocator_generated_options or
                        (sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "generated_always") and
                            sql_adapter.planHasNonZeroToken(entry.plan, ":options="));
                },
                .create_schema_namespace => self.ddl_schema_namespace_create = true,
                .rename_schema_namespace => self.ddl_schema_namespace_rename = true,
                .drop_schema_namespace => self.ddl_schema_namespace_drop = true,
                .create_extension => self.ddl_extension_create = true,
                .alter_extension_update, .drop_extension => {},
                .create_function => {
                    self.ddl_function_create = true;
                    self.ddl_function_replace = self.ddl_function_replace or sql_adapter.planHasExactBoolToken(entry.plan, ":replace=", true);
                },
                .drop_function => self.ddl_function_drop = true,
                .create_procedure => self.ddl_procedure_create = true,
                .drop_procedure => self.ddl_procedure_drop = true,
                .create_role => self.ddl_role_create = true,
                .alter_role => self.ddl_role_alter = true,
                .drop_role => self.ddl_role_drop = true,
                .grant_privilege => self.ddl_privilege_grant = true,
                .revoke_privilege => self.ddl_privilege_revoke = true,
                .copy_from => self.ddl_copy_from = true,
                .copy_to => self.ddl_copy_to = true,
                .create_partitioned_table => self.ddl_partition_create_parent = true,
                .create_table_partition => self.ddl_partition_create_child = true,
                .attach_table_partition => self.ddl_partition_attach = true,
                .detach_table_partition => self.ddl_partition_detach = true,
                .enable_row_security => self.ddl_row_security_enable = true,
                .disable_row_security => self.ddl_row_security_disable = true,
                .create_row_policy => self.ddl_row_security_create_policy = true,
                .drop_row_policy => self.ddl_row_security_drop_policy = true,
                .create_database => self.ddl_database_create = true,
                .alter_database => self.ddl_database_alter = true,
                .drop_database => self.ddl_database_drop = true,
                .create_tablespace => self.ddl_tablespace_create = true,
                .rename_tablespace => self.ddl_tablespace_rename = true,
                .drop_tablespace => self.ddl_tablespace_drop = true,
                .listen_notification => self.ddl_notification_listen = true,
                .notify_notification => self.ddl_notification_notify = true,
                .unlisten_notification => self.ddl_notification_unlisten = true,
                .create_publication => self.ddl_publication_create = true,
                .alter_publication => self.ddl_publication_alter = true,
                .drop_publication => self.ddl_publication_drop = true,
                .create_subscription => self.ddl_subscription_create = true,
                .alter_subscription => self.ddl_subscription_alter = true,
                .drop_subscription => self.ddl_subscription_drop = true,
                .create_collation => self.ddl_collation_create = true,
                .rename_collation => self.ddl_collation_rename = true,
                .drop_collation => self.ddl_collation_drop = true,
                .create_operator => self.ddl_operator_create = true,
                .drop_operator => self.ddl_operator_drop = true,
                .create_aggregate => self.ddl_aggregate_create = true,
                .drop_aggregate => self.ddl_aggregate_drop = true,
                .create_cast => self.ddl_cast_create = true,
                .drop_cast => self.ddl_cast_drop = true,
                .vacuum_maintenance => self.ddl_vacuum_maintenance = true,
                .analyze_maintenance => self.ddl_analyze_maintenance = true,
                .reindex_maintenance => self.ddl_reindex_maintenance = true,
                .cluster_maintenance => self.ddl_cluster_maintenance = true,
                .prepare_statement => {
                    self.ddl_prepare_statement = true;
                    self.ddl_prepare_cte_write_statement = self.ddl_prepare_cte_write_statement or
                        std.mem.startsWith(u8, entry.sql, "PREPARE ") and
                            std.mem.indexOf(u8, entry.sql, " AS WITH ") != null and
                            sql_adapter.planHasExactStringToken(entry.plan, ":subject=", "write");
                },
                .execute_statement => self.ddl_execute_statement = true,
                .deallocate_statement => self.ddl_deallocate_statement = true,
                .declare_cursor => self.ddl_declare_cursor = true,
                .fetch_cursor => self.ddl_fetch_cursor = true,
                .close_cursor => self.ddl_close_cursor = true,
                .savepoint_transaction => self.ddl_savepoint_transaction = true,
                .release_savepoint => self.ddl_release_savepoint = true,
                .rollback_to_savepoint => self.ddl_rollback_to_savepoint = true,
                .comment_metadata => {
                    self.ddl_comment_table = self.ddl_comment_table or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "table");
                    self.ddl_comment_column = self.ddl_comment_column or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "column");
                    self.ddl_comment_index = self.ddl_comment_index or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "index");
                    self.ddl_comment_constraint = self.ddl_comment_constraint or sql_adapter.planHasExactStringToken(entry.plan, ":kind=", "constraint");
                },
                .table_lock => self.ddl_table_lock = true,
                .constraint_mode => self.ddl_constraint_mode = true,
                .transaction_mode => {
                    self.ddl_set_transaction_mode = self.ddl_set_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "set_transaction");
                    self.ddl_start_transaction_mode = self.ddl_start_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "start_transaction");
                    self.ddl_begin_transaction_mode = self.ddl_begin_transaction_mode or sql_adapter.planHasExactStringToken(entry.plan, ":starter=", "begin");
                    self.ddl_transaction_deferrable_true = self.ddl_transaction_deferrable_true or sql_adapter.planHasExactStringToken(entry.plan, ":deferrable=", "true");
                    self.ddl_transaction_deferrable_false = self.ddl_transaction_deferrable_false or sql_adapter.planHasExactStringToken(entry.plan, ":deferrable=", "false");
                },
                .advisory_lock => {
                    self.ddl_advisory_lock = self.ddl_advisory_lock or sql_adapter.planHasExactStringToken(entry.plan, ":action=", "lock");
                    self.ddl_advisory_unlock = self.ddl_advisory_unlock or sql_adapter.planHasExactStringToken(entry.plan, ":action=", "unlock");
                },
                .set_search_path => self.session_set_search_path = true,
                .reset_search_path => self.session_reset_search_path = true,
                .show_search_path => self.session_show_search_path = true,
                .discard_all => self.session_discard = true,
                .create_index => {
                    self.ddl_create_index = true;
                    self.ddl_create_covering_index = self.ddl_create_covering_index or sql_adapter.planHasNonZeroToken(entry.plan, ":include=");
                },
                .drop_index => self.ddl_drop_index = true,
                .drop_table => {
                    self.ddl_drop_table = true;
                    self.ddl_drop_table_cascade = self.ddl_drop_table_cascade or sql_adapter.planHasExactBoolToken(entry.plan, ":cascade=", true);
                },
                .alter_table => {
                    self.ddl_alter_table = true;
                    self.ddl_add_column_default_rewrite = self.ddl_add_column_default_rewrite or
                        std.mem.indexOf(u8, entry.sql, "ADD COLUMN") != null and
                            std.mem.indexOf(u8, entry.sql, "DEFAULT") != null and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", true) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", true) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", true);
                    self.ddl_add_unvalidated_unique = self.ddl_add_unvalidated_unique or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_unique=", 1);
                    self.ddl_add_unvalidated_fk = self.ddl_add_unvalidated_fk or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_fk=", 1);
                    self.ddl_add_unvalidated_check = self.ddl_add_unvalidated_check or sql_adapter.appliedPlanHasExactUsizeToken(entry.applied_plan, "unvalidated_check=", 1);
                    self.ddl_validate_constraint = self.ddl_validate_constraint or std.mem.indexOf(u8, entry.sql, "VALIDATE CONSTRAINT") != null;
                    self.ddl_drop_constraint = self.ddl_drop_constraint or std.mem.indexOf(u8, entry.sql, "DROP CONSTRAINT") != null;
                    self.ddl_drop_column = self.ddl_drop_column or std.mem.indexOf(u8, entry.sql, "DROP COLUMN") != null;
                    self.ddl_alter_column_default = self.ddl_alter_column_default or std.mem.indexOf(u8, entry.sql, "SET DEFAULT") != null or std.mem.indexOf(u8, entry.sql, "DROP DEFAULT") != null;
                    self.ddl_drop_column_default = self.ddl_drop_column_default or
                        std.mem.indexOf(u8, entry.sql, "DROP DEFAULT") != null and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false);
                    self.ddl_alter_column_not_null = self.ddl_alter_column_not_null or std.mem.indexOf(u8, entry.sql, "SET NOT NULL") != null or std.mem.indexOf(u8, entry.sql, "DROP NOT NULL") != null;
                    self.ddl_drop_column_not_null = self.ddl_drop_column_not_null or
                        std.mem.indexOf(u8, entry.sql, "DROP NOT NULL") != null and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rebuild=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "validation=", false) and
                            sql_adapter.appliedPlanHasExactBoolToken(entry.applied_plan, "rewrite=", false);
                    self.ddl_alter_column_type = self.ddl_alter_column_type or std.mem.indexOf(u8, entry.sql, " TYPE ") != null or std.mem.indexOf(u8, entry.sql, " SET DATA TYPE ") != null;
                    self.ddl_rename_column = self.ddl_rename_column or std.mem.indexOf(u8, entry.sql, "RENAME COLUMN") != null;
                    self.ddl_rename_constraint = self.ddl_rename_constraint or std.mem.indexOf(u8, entry.sql, "RENAME CONSTRAINT") != null;
                    self.ddl_drop_update_policy = self.ddl_drop_update_policy or std.mem.indexOf(u8, entry.sql, "DROP TRIGGER") != null;
                },
                .create_update_policy => self.ddl_create_update_policy = true,
            }
        } else if (entry.family == .adapter_noop_ddl) {
            self.adapter_noop_transaction = self.adapter_noop_transaction or std.mem.eql(u8, entry.classification_reason, "transaction_control");
            self.adapter_noop_transaction_commit = self.adapter_noop_transaction_commit or
                std.mem.eql(u8, entry.classification_reason, "transaction_control") and
                    std.ascii.startsWithIgnoreCase(entry.sql, "COMMIT");
            self.adapter_noop_transaction_rollback = self.adapter_noop_transaction_rollback or
                std.mem.eql(u8, entry.classification_reason, "transaction_control") and
                    std.ascii.startsWithIgnoreCase(entry.sql, "ROLLBACK");
            self.adapter_noop_session = self.adapter_noop_session or std.mem.eql(u8, entry.classification_reason, "session_setting");
            self.adapter_noop_session_probe = self.adapter_noop_session_probe or
                std.mem.eql(u8, entry.classification_reason, "session_setting") and
                    (std.ascii.startsWithIgnoreCase(entry.sql, "RESET") or std.ascii.startsWithIgnoreCase(entry.sql, "SHOW"));
            self.adapter_noop_schema_namespace = self.adapter_noop_schema_namespace or std.mem.eql(u8, entry.classification_reason, "schema_namespace");
            self.adapter_noop_extension = self.adapter_noop_extension or std.mem.eql(u8, entry.classification_reason, "extension");
            self.session_discard = self.session_discard or
                std.mem.eql(u8, entry.classification_reason, "session_setting") and
                    std.ascii.startsWithIgnoreCase(entry.sql, "DISCARD");
        }

        self.scalar_membership = self.scalar_membership or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":in=",
            "_in=",
            ":source_in=",
            "_source_in=",
            ":array_any=",
            "_array_any=",
        });
        self.boolean_is_predicate = self.boolean_is_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (std.mem.indexOf(u8, entry.sql, " IS TRUE") != null or
                    std.mem.indexOf(u8, entry.sql, " IS FALSE") != null);
        self.boolean_is_not_predicate = self.boolean_is_not_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":or=") and
                (std.mem.indexOf(u8, entry.sql, " IS NOT TRUE") != null or
                    std.mem.indexOf(u8, entry.sql, " IS NOT FALSE") != null);
        self.boolean_unknown_predicate = self.boolean_unknown_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (std.mem.indexOf(u8, entry.sql, " IS UNKNOWN") != null or
                    std.mem.indexOf(u8, entry.sql, " IS NOT UNKNOWN") != null);
        self.postfix_null_test_predicate = self.postfix_null_test_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":pred=") and
                (std.mem.indexOf(u8, entry.sql, " ISNULL") != null or
                    std.mem.indexOf(u8, entry.sql, " NOTNULL") != null);
        self.expression_postfix_null_test_predicate = self.expression_postfix_null_test_predicate or
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
                (std.mem.indexOf(u8, entry.sql, " ISNULL") != null or
                    std.mem.indexOf(u8, entry.sql, " NOTNULL") != null);
        self.json_access_path = self.json_access_path or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":json_eq=",
            "_json_eq=",
            ":json_contains=",
            "_json_contains=",
            ":json_exists=",
            "_json_exists=",
            ":source_json_eq=",
            ":source_json_contains=",
            ":source_json_exists=",
        });
        self.array_access_path = self.array_access_path or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":array_contains=",
            "_array_contains=",
            ":array_eq=",
            "_array_eq=",
            ":source_array_contains=",
            ":source_array_eq=",
        });
        self.text_pattern = self.text_pattern or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":text_pattern=",
            "_text_pattern=",
            ":source_text_pattern=",
        });
        self.query_access_or_predicates = self.query_access_or_predicates or
            entry.family == .query and sql_adapter.planHasNonZeroToken(entry.plan, ":access_or=");
        self.query_array_overlap_access_or = self.query_array_overlap_access_or or
            entry.family == .query and
                std.mem.indexOf(u8, entry.sql, "tags && ARRAY") != null and
                sql_adapter.planHasNonZeroToken(entry.plan, ":access_or=");
        self.query_access_not_predicates = self.query_access_not_predicates or
            entry.family == .query and sql_adapter.planHasNonZeroToken(entry.plan, ":access_not=");
        self.read_row_lock_nowait = self.read_row_lock_nowait or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "nowait", "no_key_update_nowait", "share_nowait", "key_share_nowait" }));
        self.read_row_lock_share = self.read_row_lock_share or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "share", "share_nowait", "share_skip_locked" }));
        self.read_row_lock_key_share = self.read_row_lock_key_share or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "key_share", "key_share_nowait", "key_share_skip_locked" }));
        self.query_row_lock_no_key_update = self.query_row_lock_no_key_update or
            (entry.family == .query and
                sql_adapter.planHasAnyExactStringToken(entry.plan, ":claim=", &.{ "no_key_update", "no_key_update_nowait", "no_key_update_skip_locked" }));
        self.expression_predicate = self.expression_predicate or sql_adapter.planHasAnyNonZeroToken(entry.plan, &.{
            ":expr_pred=",
            "_expr_pred=",
            ":source_expr_pred=",
            ":expr_or=",
            "_expr_or=",
            ":source_expr_or=",
            ":expr_not=",
            "_expr_not=",
            ":source_expr_not=",
            ":expr_array=",
            "_expr_array=",
            ":source_expr_array=",
            ":having_expr=",
            ":having_any=",
            ":having_not=",
            ":filter_expr=",
            ":filter_groups=",
        });
        self.mixed_scalar_expression_or = self.mixed_scalar_expression_or or
            entry.family == .query and
                sql_adapter.planHasNonZeroToken(entry.plan, ":expr_or=") and
                std.mem.indexOf(u8, entry.sql, "id = 'u1' OR lower(email)") != null;
        self.expression_order = self.expression_order or sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr=") or sql_adapter.planHasNonZeroToken(entry.plan, "_order_expr=");
        self.query_order_using_operator = self.query_order_using_operator or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.aggregate_order_using_operator = self.aggregate_order_using_operator or (entry.family == .aggregate and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.join_order_using_operator = self.join_order_using_operator or (entry.family == .join and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.lateral_order_using_operator = self.lateral_order_using_operator or (entry.family == .lateral and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.window_order_using_operator = self.window_order_using_operator or (entry.family == .window and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order="));
        self.update_source_order_using_operator = self.update_source_order_using_operator or (entry.family == .update_source and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_order="));
        self.delete_source_order_using_operator = self.delete_source_order_using_operator or (entry.family == .delete_source and
            std.mem.indexOf(u8, entry.sql, " USING ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":source_order="));
        self.query_fixed_interval_expression = self.query_fixed_interval_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "INTERVAL '1 hour'") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_calendar_interval_expression = self.query_calendar_interval_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "INTERVAL '1 month'") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_mixed_interval_expression = self.query_mixed_interval_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "INTERVAL '1 month 1 day'") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_now_expression = self.query_now_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "now()") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_timestamp_expression = self.query_current_timestamp_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "CURRENT_TIMESTAMP AS") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_timestamp_precision_expression = self.query_current_timestamp_precision_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "CURRENT_TIMESTAMP(6)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_current_date_expression = self.query_current_date_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "CURRENT_DATE") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_uuid_generation_expression = self.query_uuid_generation_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "gen_random_uuid()") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_uuid_generate_v4_expression = self.query_uuid_generate_v4_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "uuid_generate_v4()") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_substring_expression = self.query_substring_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "substring(status") != null and
            std.mem.indexOf(u8, entry.sql, "substr(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_overlay_expression = self.query_overlay_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "overlay(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_translate_expression = self.query_translate_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "translate(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_split_part_expression = self.query_split_part_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "split_part(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_strpos_expression = self.query_strpos_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "strpos(status") != null and
            std.mem.indexOf(u8, entry.sql, "position(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_left_right_expression = self.query_left_right_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "left(status") != null and
            std.mem.indexOf(u8, entry.sql, "right(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_trim_variant_expression = self.query_trim_variant_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "btrim(status") != null and
            std.mem.indexOf(u8, entry.sql, "ltrim(status") != null and
            std.mem.indexOf(u8, entry.sql, "rtrim(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_replace_expression = self.query_regexp_replace_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "regexp_replace(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_substr_expression = self.query_regexp_substr_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "regexp_substr(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_match_expression = self.query_regexp_match_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "status ~") != null and
            std.mem.indexOf(u8, entry.sql, "email !~*") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_regexp_count_expression = self.query_regexp_count_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "regexp_count(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_regexp_instr_expression = self.query_regexp_instr_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "regexp_instr(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_pad_expression = self.query_pad_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "lpad(status") != null and
            std.mem.indexOf(u8, entry.sql, "rpad(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_repeat_expression = self.query_repeat_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "repeat(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_reverse_expression = self.query_reverse_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "reverse(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_initcap_expression = self.query_initcap_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "initcap(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_text_length_expression = self.query_text_length_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "char_length(status") != null and
            std.mem.indexOf(u8, entry.sql, "character_length(status") != null and
            std.mem.indexOf(u8, entry.sql, "octet_length(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_bit_length_expression = self.query_bit_length_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "bit_length(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_md5_expression = self.query_md5_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "md5(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_concat_ws_expression = self.query_concat_ws_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "concat_ws(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nullif_expression = self.query_nullif_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "nullif(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_extremum_expression = self.query_extremum_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "greatest(") != null and
            std.mem.indexOf(u8, entry.sql, "least(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nullable_pagination = self.query_nullable_pagination or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "LIMIT NULL") != null and
            std.mem.indexOf(u8, entry.sql, "OFFSET NULL") != null and
            sql_adapter.planHasExactStringToken(entry.plan, ":limit=", "none") and
            sql_adapter.planTokenAbsent(entry.plan, ":offset="));
        self.query_json_build_object_expression = self.query_json_build_object_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "jsonb_build_object(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_to_jsonb_expression = self.query_to_jsonb_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "to_jsonb(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_convert_from_jsonb_expression = self.query_convert_from_jsonb_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "convert_from(") != null and
            std.mem.indexOf(u8, entry.sql, "::jsonb") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_cardinality_expression = self.query_cardinality_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "cardinality(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_array_position_expression = self.query_array_position_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "array_position(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_array_positions_expression = self.query_array_positions_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "array_positions(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_array_element_transform_expression = self.query_array_element_transform_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "array_append(") != null and
            std.mem.indexOf(u8, entry.sql, "array_cat(") != null and
            std.mem.indexOf(u8, entry.sql, "array_remove(") != null and
            std.mem.indexOf(u8, entry.sql, "array_replace(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_array_to_string_expression = self.query_array_to_string_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "array_to_string(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred="));
        self.query_string_to_array_expression = self.query_string_to_array_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "string_to_array(") != null and
            (sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") or sql_adapter.planHasNonZeroToken(entry.plan, ":expr_arr=")));
        self.query_starts_with_expression = self.query_starts_with_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "starts_with(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_ends_with_expression = self.query_ends_with_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "ends_with(status") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_ascii_chr_expression = self.query_ascii_chr_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "ascii(status") != null and
            std.mem.indexOf(u8, entry.sql, "chr(amount") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_modulo_expression = self.query_modulo_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "amount % quantity") != null and
            std.mem.indexOf(u8, entry.sql, "MOD(amount + quantity") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_trunc_expression = self.query_date_trunc_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "date_trunc('hour'") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_bin_expression = self.query_date_bin_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "date_bin(") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_typed_datetime_literal_expression = self.query_typed_datetime_literal_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "TIMESTAMPTZ ") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr="));
        self.query_date_part_expression = self.query_date_part_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "date_part('hour'") != null and
            std.mem.indexOf(u8, entry.sql, "EXTRACT(dow") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_date_part_epoch_expression = self.query_date_part_epoch_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "date_part('epoch'") != null and
            std.mem.indexOf(u8, entry.sql, "EXTRACT(epoch") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.query_nested_case_fold_text_expression = self.query_nested_case_fold_text_expression or (entry.family == .query and
            std.mem.indexOf(u8, entry.sql, "lower(status || ':' || id)") != null and
            std.mem.indexOf(u8, entry.sql, "upper(status || ':' || id)") != null and
            sql_adapter.planHasNonZeroToken(entry.plan, ":expr_pred=") and
            sql_adapter.planHasNonZeroToken(entry.plan, ":order_expr="));
        self.cte_stream = self.cte_stream or uses_cte_stream;

        for (entry.returning_rows) |row_json| {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, row_json, .{});
            parsed.deinit();
            self.deterministic_returning_rows += 1;
        }
        if (entry.returning_rows.len > 0) {
            switch (entry.family) {
                .insert => self.deterministic_insert_returning_rows = true,
                .update => self.deterministic_update_returning_rows = true,
                .delete => self.deterministic_delete_returning_rows = true,
                else => {},
            }
        }
        if (uses_returning_all) {
            switch (entry.family) {
                .insert => self.returning_all_insert = true,
                .update => self.returning_all_update = true,
                .delete => self.returning_all_delete = true,
                .update_source => self.returning_all_update_source = true,
                .delete_source => self.returning_all_delete_source = true,
                .update_joined_source => self.returning_all_update_joined_source = true,
                .delete_joined_source => self.returning_all_delete_joined_source = true,
                else => {},
            }
        }
        if (uses_insert_conflict) {
            self.conflict_do_update = self.conflict_do_update or (std.mem.indexOf(u8, entry.sql, "DO UPDATE") != null and sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_default_update = self.conflict_default_update or std.mem.indexOf(u8, entry.sql, "SET status = DEFAULT") != null;
            self.conflict_coalesce_existing_update = self.conflict_coalesce_existing_update or
                std.mem.indexOf(u8, entry.sql, "coalesce(excluded.") != null;
            self.conflict_numeric_expression_update = self.conflict_numeric_expression_update or
                std.mem.indexOf(u8, entry.sql, "greatest(amount, excluded.amount") != null;
            self.conflict_case_expression_update = self.conflict_case_expression_update or
                std.mem.indexOf(u8, entry.sql, "CASE WHEN excluded.amount > amount") != null;
            self.conflict_current_timestamp_precision = self.conflict_current_timestamp_precision or
                std.mem.indexOf(u8, entry.sql, "CURRENT_TIMESTAMP(6)") != null;
            self.conflict_current_date_update = self.conflict_current_date_update or
                std.mem.indexOf(u8, entry.sql, "CURRENT_DATE") != null;
            self.conflict_uuid_generation_update = self.conflict_uuid_generation_update or
                std.mem.indexOf(u8, entry.sql, "uuid_generate_v4()") != null or
                std.mem.indexOf(u8, entry.sql, "gen_random_uuid()") != null;
            self.conflict_text_expression_update = self.conflict_text_expression_update or
                std.mem.indexOf(u8, entry.sql, "length(excluded.next_status)") != null or
                std.mem.indexOf(u8, entry.sql, "char_length(excluded.next_status)") != null or
                std.mem.indexOf(u8, entry.sql, "character_length(excluded.next_status)") != null or
                std.mem.indexOf(u8, entry.sql, "octet_length(excluded.next_status)") != null or
                std.mem.indexOf(u8, entry.sql, "bit_length(excluded.next_status)") != null;
            self.conflict_octet_length_expression_update = self.conflict_octet_length_expression_update or
                std.mem.indexOf(u8, entry.sql, "octet_length(excluded.next_status)") != null;
            self.conflict_bit_length_expression_update = self.conflict_bit_length_expression_update or
                std.mem.indexOf(u8, entry.sql, "bit_length(excluded.next_status)") != null;
            self.conflict_regexp_replace_expression_update = self.conflict_regexp_replace_expression_update or
                std.mem.indexOf(u8, entry.sql, "regexp_replace(excluded.status") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_match_expression_update = self.conflict_regexp_match_expression_update or
                (std.mem.indexOf(u8, entry.sql, "regexp_like(excluded.status") != null or
                    std.mem.indexOf(u8, entry.sql, "regexp_match(excluded.status") != null) and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_count_expression_update = self.conflict_regexp_count_expression_update or
                std.mem.indexOf(u8, entry.sql, "regexp_count(excluded.status") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_instr_expression_update = self.conflict_regexp_instr_expression_update or
                std.mem.indexOf(u8, entry.sql, "regexp_instr(excluded.status") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_regexp_substr_expression_update = self.conflict_regexp_substr_expression_update or
                std.mem.indexOf(u8, entry.sql, "regexp_substr(excluded.status") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms=");
            self.conflict_nested_text_expression_update = self.conflict_nested_text_expression_update or
                std.mem.indexOf(u8, entry.sql, "length(lower(excluded.next_status || '-' || status))") != null or
                std.mem.indexOf(u8, entry.sql, "char_length(lower(excluded.next_status || '-' || status))") != null or
                std.mem.indexOf(u8, entry.sql, "character_length(lower(excluded.next_status || '-' || status))") != null;
            self.conflict_jsonb_update = self.conflict_jsonb_update or std.mem.indexOf(u8, entry.sql, "jsonb") != null;
            self.conflict_jsonb_concat_update = self.conflict_jsonb_concat_update or
                std.mem.indexOf(u8, entry.sql, "metadata ||") != null and
                    std.mem.indexOf(u8, entry.sql, "::jsonb") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, ":ops=");
            self.multi_row_conflict_do_nothing = self.multi_row_conflict_do_nothing or (uses_multi_row_insert and std.mem.indexOf(u8, entry.sql, "DO NOTHING") != null);
            self.multi_row_conflict_do_nothing_duplicate_target = self.multi_row_conflict_do_nothing_duplicate_target or (uses_multi_row_insert and
                entry.resolver_exists == false and
                std.mem.indexOf(u8, entry.sql, "DO NOTHING") != null and
                sql_adapter.writePlanHasCounts(entry.plan, 1, 0));
            self.conflict_returning_expression = self.conflict_returning_expression or sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr=");
            self.conflict_do_nothing_returning_all = self.conflict_do_nothing_returning_all or (uses_returning_all and
                sql_adapter.writePlanHasCounts(entry.plan, 0, 0) and
                std.mem.indexOf(u8, entry.sql, "DO NOTHING") != null);
            self.conflict_guard_where = self.conflict_guard_where or uses_conflict_where;
            self.conflict_guard_where_skip = self.conflict_guard_where_skip or (uses_conflict_where and
                sql_adapter.writePlanHasCounts(entry.plan, 0, 0) and
                sql_adapter.planHasExactUsizeToken(entry.plan, ":returning_rows=", 0));
            self.conflict_interval_update = self.conflict_interval_update or
                std.mem.indexOf(u8, entry.sql, "INTERVAL '1 second'") != null;
            self.conflict_mixed_interval_update = self.conflict_mixed_interval_update or
                std.mem.indexOf(u8, entry.sql, "INTERVAL '1 month 1 day'") != null;
            self.conflict_date_bin_update = self.conflict_date_bin_update or
                (std.mem.indexOf(u8, entry.sql, "date_bin(") != null and
                    std.mem.indexOf(u8, entry.sql, "DO UPDATE SET") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_typed_datetime_literal_update = self.conflict_typed_datetime_literal_update or
                (std.mem.indexOf(u8, entry.sql, "DO UPDATE SET updated_at_ns = TIMESTAMPTZ ") != null and
                    sql_adapter.planHasNonZeroToken(entry.plan, "transforms="));
            self.conflict_row_assignment = self.conflict_row_assignment or
                std.mem.indexOf(u8, entry.sql, "DO UPDATE SET (status, quantity)") != null;
            self.conflict_row_assignment_default = self.conflict_row_assignment_default or
                (std.mem.indexOf(u8, entry.sql, "DO UPDATE SET (status, quantity)") != null and
                    std.mem.indexOf(u8, entry.sql, "DEFAULT") != null);
            self.conflict_row_assignment_constructor = self.conflict_row_assignment_constructor or
                (std.mem.indexOf(u8, entry.sql, "DO UPDATE SET (status, quantity)") != null and
                    std.mem.indexOf(u8, entry.sql, " = ROW(") != null);
            self.conflict_boolean_expression_update = self.conflict_boolean_expression_update or
                std.mem.indexOf(u8, entry.sql, "SET enabled = excluded.enabled OR false") != null;
            self.schema_default_primary_named_conflict_target = self.schema_default_primary_named_conflict_target or
                std.mem.indexOf(u8, entry.sql, "ON CONFLICT ON CONSTRAINT usage_records_pkey") != null;
            self.schema_custom_primary_named_conflict_target = self.schema_custom_primary_named_conflict_target or
                (appParityAnyStringContains(entry.apply_setup_sql, "RENAME CONSTRAINT usage_records_pkey TO usage_records_id_pk") and
                    std.mem.indexOf(u8, entry.sql, "ON CONFLICT ON CONSTRAINT usage_records_id_pk") != null);
            self.schema_unique_conflict_target = self.schema_unique_conflict_target or (appParityAnyStringContains(entry.apply_setup_sql, "email text UNIQUE") and
                std.mem.indexOf(u8, entry.sql, "ON CONFLICT (email)") != null);
            self.schema_partial_unique_conflict_target = self.schema_partial_unique_conflict_target or (appParityAnyStringContains(entry.apply_setup_sql, "CREATE UNIQUE INDEX") and
                appParityAnyStringContains(entry.apply_setup_sql, " WHERE ") and
                std.mem.indexOf(u8, entry.sql, "ON CONFLICT (email) WHERE") != null);
            self.schema_expression_unique_conflict_target = self.schema_expression_unique_conflict_target or (appParityAnyStringContains(entry.apply_setup_sql, "CREATE UNIQUE INDEX") and
                (appParityAnyStringContains(entry.apply_setup_sql, "lower(") or appParityAnyStringContains(entry.apply_setup_sql, "upper(")) and
                (std.mem.indexOf(u8, entry.sql, "ON CONFLICT (lower(") != null or std.mem.indexOf(u8, entry.sql, "ON CONFLICT (upper(") != null));
            self.schema_mixed_expression_unique_conflict_target = self.schema_mixed_expression_unique_conflict_target or (appParityAnyStringContains(entry.apply_setup_sql, "CREATE UNIQUE INDEX") and
                appParityAnyStringContains(entry.apply_setup_sql, "tenant_id, lower(") and
                std.mem.indexOf(u8, entry.sql, "ON CONFLICT (tenant_id, lower(") != null);
        } else if (entry.family == .unsupported_insert) {
            self.unsupported_duplicate_row_batch_target = self.unsupported_duplicate_row_batch_target or std.mem.eql(u8, entry.classification_reason, "duplicate_row_batch_target");
            self.unsupported_duplicate_conflict_update_target = self.unsupported_duplicate_conflict_update_target or std.mem.eql(u8, entry.classification_reason, "duplicate_conflict_update_target");
            self.unsupported_invalid_expression_conflict_target = self.unsupported_invalid_expression_conflict_target or std.mem.eql(u8, entry.classification_reason, "invalid_expression_conflict_target");
            self.unsupported_invalid_named_conflict_target = self.unsupported_invalid_named_conflict_target or std.mem.eql(u8, entry.classification_reason, "invalid_named_conflict_target");
            self.unsupported_unvalidated_unique_conflict_target = self.unsupported_unvalidated_unique_conflict_target or std.mem.eql(u8, entry.classification_reason, "enforced_unique_conflict_target");
        }
        if (entry.family == .insert and !uses_insert_conflict) {
            self.multi_row_insert = self.multi_row_insert or uses_multi_row_insert;
            self.insert_typed_datetime_literal = self.insert_typed_datetime_literal or
                std.mem.indexOf(u8, entry.sql, "TIMESTAMPTZ ") != null;
        }
        self.point_update_expression_partial_unique_selector = self.point_update_expression_partial_unique_selector or
            (entry.family == .update and std.mem.indexOf(u8, entry.name, "expression partial unique selector") != null);
        self.point_delete_expression_partial_unique_selector = self.point_delete_expression_partial_unique_selector or
            (entry.family == .delete and std.mem.indexOf(u8, entry.name, "expression partial unique selector") != null);
        self.insert_source_cross_table_source_schema = self.insert_source_cross_table_source_schema or
            (entry.family == .insert_source and
                entry.source_schema_json.len > 0 and
                sql_adapter.planHasExactStringToken(entry.plan, ":source_table=", "archived_records"));
        self.insert_source_expression_assignment = self.insert_source_expression_assignment or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":assignment_expr="));
        self.insert_source_regexp_expression_assignment = self.insert_source_regexp_expression_assignment or
            (entry.family == .insert_source and
                std.mem.indexOf(u8, entry.sql, "regexp_like(status") != null and
                std.mem.indexOf(u8, entry.sql, "regexp_substr(status") != null and
                std.mem.indexOf(u8, entry.sql, "regexp_count(status") != null and
                std.mem.indexOf(u8, entry.sql, "regexp_instr(status") != null and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":assignment_expr=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.insert_source_computed_pattern_source = self.insert_source_computed_pattern_source or
            (entry.family == .insert_source and
                uses_computed_pattern and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_pred="));
        self.insert_source_expression_or_source = self.insert_source_expression_or_source or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_or="));
        self.insert_source_expression_not_source = self.insert_source_expression_not_source or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":source_expr_not="));
        self.insert_source_returning_all_expression = self.insert_source_returning_all_expression or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_all=") and
                sql_adapter.planHasNonZeroToken(entry.plan, ":returning_expr="));
        self.insert_source_conflict_default_update = self.insert_source_conflict_default_update or
            (entry.family == .insert_source and
                std.mem.indexOf(u8, entry.sql, "SET status = DEFAULT") != null and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_ops="));
        self.insert_source_conflict_json_set_expression = self.insert_source_conflict_json_set_expression or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_json_set_expr="));
        self.insert_source_conflict_regexp_expression = self.insert_source_conflict_regexp_expression or
            (entry.family == .insert_source and
                std.mem.indexOf(u8, entry.sql, "regexp_substr(excluded.status") != null and
                std.mem.indexOf(u8, entry.sql, "regexp_count(excluded.status") != null and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_patch_expr="));
        self.insert_source_conflict_boolean_is_not_guard = self.insert_source_conflict_boolean_is_not_guard or
            (entry.family == .insert_source and
                sql_adapter.planHasNonZeroToken(entry.plan, ":conflict_where_any=") and
                std.mem.indexOf(u8, entry.sql, " IS NOT TRUE") != null);
        self.joined_source_cross_table_source_schema = self.joined_source_cross_table_source_schema or
            ((entry.family == .update_joined_source or entry.family == .delete_joined_source) and
                entry.source_schema_json.len > 0 and
                sql_adapter.planHasExactStringToken(entry.plan, ":source=", "source_records"));
        self.read_join_cross_table_source_schema = self.read_join_cross_table_source_schema or
            (entry.family == .join and
                entry.source_schema_json.len > 0 and
                sql_adapter.planHasExactStringToken(entry.plan, ":right=", "customer_records"));
        self.read_lateral_cross_table_source_schema = self.read_lateral_cross_table_source_schema or
            (entry.family == .lateral and
                entry.source_schema_json.len > 0 and
                sql_adapter.planHasExactStringToken(entry.plan, ":right=", "balance_records"));
        self.merge_cross_table_source_schema = self.merge_cross_table_source_schema or
            (entry.family == .merge_mutation and
                entry.source_schema_json.len > 0 and
                sql_adapter.planHasExactStringToken(entry.plan, ":source=", "archived_records"));
    }

    pub fn expectComplete(self: @This()) !void {
        try std.testing.expect(self.ddl);
        try std.testing.expect(self.ddl_table_clone);
        try std.testing.expect(self.read);
        try std.testing.expect(self.read_query);
        try std.testing.expect(self.read_aggregate);
        try std.testing.expect(self.read_join);
        try std.testing.expect(self.read_lateral);
        try std.testing.expect(self.read_window);
        try std.testing.expect(self.read_cte_query_expression);
        try std.testing.expect(self.read_cte_aggregate_expression);
        try std.testing.expect(self.read_cte_window_expression);
        try std.testing.expect(self.read_join_cross_table_source_schema_classifier);
        try std.testing.expect(self.read_lateral_cross_table_source_schema_classifier);
        try std.testing.expect(self.query);
        try std.testing.expect(self.aggregate);
        try std.testing.expect(self.join);
        try std.testing.expect(self.lateral);
        try std.testing.expect(self.window);
        try std.testing.expect(self.explain);
        try std.testing.expect(self.explain_options);
        try std.testing.expect(self.explain_analyze);
        try std.testing.expect(self.explain_write);
        try std.testing.expect(self.relation_population_select_into);
        try std.testing.expect(self.relation_population_create_table_as);
        try std.testing.expect(self.insert);
        try std.testing.expect(self.insert_source);
        try std.testing.expect(self.insert_source_expression_assignment);
        try std.testing.expect(self.insert_source_regexp_expression_assignment);
        try std.testing.expect(self.insert_source_computed_pattern_source);
        try std.testing.expect(self.insert_source_expression_or_source);
        try std.testing.expect(self.insert_source_expression_not_source);
        try std.testing.expect(self.insert_source_returning_all_expression);
        try std.testing.expect(self.insert_source_conflict_default_update);
        try std.testing.expect(self.insert_source_conflict_regexp_expression);
        try std.testing.expect(self.insert_source_cross_table_source_schema);
        try std.testing.expect(self.joined_source_cross_table_source_schema);
        try std.testing.expect(self.read_join_cross_table_source_schema);
        try std.testing.expect(self.read_lateral_cross_table_source_schema);
        try std.testing.expect(self.merge_cross_table_source_schema);
        try std.testing.expect(self.update);
        try std.testing.expect(self.delete);
        try std.testing.expect(self.update_source);
        try std.testing.expect(self.delete_source);
        try std.testing.expect(self.truncate_source);
        try std.testing.expect(self.update_joined_source);
        try std.testing.expect(self.update_joined_source_cte_mutation);
        try std.testing.expect(self.delete_joined_source);
        try std.testing.expect(self.delete_joined_source_cte_mutation);
        try std.testing.expect(self.adapter_noop_ddl);
        try std.testing.expect(self.unsupported_query);
        try std.testing.expect(self.unsupported_read);
        try std.testing.expect(self.unsupported_ddl);
        try std.testing.expect(self.unsupported_write);
        try std.testing.expect(self.unsupported_insert);
        try std.testing.expect(self.unsupported_update);
        try std.testing.expect(self.unsupported_update_source);
        try std.testing.expect(self.unsupported_delete);
        try std.testing.expect(self.unsupported_update_joined_source);
        try std.testing.expect(self.unsupported_delete_joined_source);
        try std.testing.expect(self.unsupported_merge_mutation);
        try std.testing.expect(self.ddl_view_create);
        try std.testing.expect(self.ddl_view_rename);
        try std.testing.expect(self.ddl_view_drop);
        try std.testing.expect(self.ddl_materialized_view_create);
        try std.testing.expect(self.ddl_materialized_view_refresh);
        try std.testing.expect(self.ddl_materialized_view_drop);
        try std.testing.expect(self.ddl_relation_lifetime_temporary);
        try std.testing.expect(self.ddl_relation_lifetime_unlogged);
        try std.testing.expect(self.ddl_enum_type_create);
        try std.testing.expect(self.ddl_enum_type_add_value);
        try std.testing.expect(self.ddl_enum_type_drop);
        try std.testing.expect(self.ddl_domain_create);
        try std.testing.expect(self.ddl_domain_alter);
        try std.testing.expect(self.ddl_domain_drop);
        try std.testing.expect(self.ddl_sequence_create);
        try std.testing.expect(self.ddl_sequence_create_typed_owned);
        try std.testing.expect(self.ddl_sequence_alter);
        try std.testing.expect(self.ddl_sequence_alter_typed_owned);
        try std.testing.expect(self.ddl_sequence_drop);
        try std.testing.expect(self.ddl_schema_namespace_create);
        try std.testing.expect(self.ddl_schema_namespace_rename);
        try std.testing.expect(self.ddl_schema_namespace_drop);
        try std.testing.expect(self.ddl_extension_create);
        try std.testing.expect(self.ddl_function_create);
        try std.testing.expect(self.ddl_function_replace);
        try std.testing.expect(self.ddl_function_drop);
        try std.testing.expect(self.ddl_procedure_create);
        try std.testing.expect(self.ddl_procedure_drop);
        try std.testing.expect(self.ddl_role_create);
        try std.testing.expect(self.ddl_role_alter);
        try std.testing.expect(self.ddl_role_drop);
        try std.testing.expect(self.ddl_privilege_grant);
        try std.testing.expect(self.ddl_privilege_revoke);
        try std.testing.expect(self.ddl_copy_from);
        try std.testing.expect(self.ddl_copy_to);
        try std.testing.expect(self.ddl_partition_create_parent);
        try std.testing.expect(self.ddl_partition_create_child);
        try std.testing.expect(self.ddl_partition_attach);
        try std.testing.expect(self.ddl_partition_detach);
        try std.testing.expect(self.ddl_row_security_enable);
        try std.testing.expect(self.ddl_row_security_disable);
        try std.testing.expect(self.ddl_row_security_create_policy);
        try std.testing.expect(self.ddl_row_security_drop_policy);
        try std.testing.expect(self.ddl_database_create);
        try std.testing.expect(self.ddl_database_alter);
        try std.testing.expect(self.ddl_database_drop);
        try std.testing.expect(self.ddl_tablespace_create);
        try std.testing.expect(self.ddl_tablespace_rename);
        try std.testing.expect(self.ddl_tablespace_drop);
        try std.testing.expect(self.ddl_notification_listen);
        try std.testing.expect(self.ddl_notification_notify);
        try std.testing.expect(self.ddl_notification_unlisten);
        try std.testing.expect(self.ddl_publication_create);
        try std.testing.expect(self.ddl_publication_alter);
        try std.testing.expect(self.ddl_publication_drop);
        try std.testing.expect(self.ddl_subscription_create);
        try std.testing.expect(self.ddl_subscription_alter);
        try std.testing.expect(self.ddl_subscription_drop);
        try std.testing.expect(self.ddl_collation_create);
        try std.testing.expect(self.ddl_collation_rename);
        try std.testing.expect(self.ddl_collation_drop);
        try std.testing.expect(self.ddl_operator_create);
        try std.testing.expect(self.ddl_operator_drop);
        try std.testing.expect(self.ddl_aggregate_create);
        try std.testing.expect(self.ddl_aggregate_drop);
        try std.testing.expect(self.ddl_cast_create);
        try std.testing.expect(self.ddl_cast_drop);
        try std.testing.expect(self.ddl_vacuum_maintenance);
        try std.testing.expect(self.ddl_analyze_maintenance);
        try std.testing.expect(self.ddl_reindex_maintenance);
        try std.testing.expect(self.ddl_cluster_maintenance);
        try std.testing.expect(self.ddl_prepare_statement);
        try std.testing.expect(self.ddl_prepare_cte_write_statement);
        try std.testing.expect(self.ddl_execute_statement);
        try std.testing.expect(self.ddl_deallocate_statement);
        try std.testing.expect(self.ddl_declare_cursor);
        try std.testing.expect(self.ddl_fetch_cursor);
        try std.testing.expect(self.ddl_close_cursor);
        try std.testing.expect(self.ddl_savepoint_transaction);
        try std.testing.expect(self.ddl_release_savepoint);
        try std.testing.expect(self.ddl_rollback_to_savepoint);
        try std.testing.expect(self.ddl_identity_allocator_serial);
        try std.testing.expect(self.ddl_identity_allocator_generated);
        try std.testing.expect(self.ddl_identity_allocator_generated_options);
        try std.testing.expect(self.ddl_table_lock);
        try std.testing.expect(self.ddl_constraint_mode);
        try std.testing.expect(self.ddl_set_transaction_mode);
        try std.testing.expect(self.ddl_start_transaction_mode);
        try std.testing.expect(self.ddl_begin_transaction_mode);
        try std.testing.expect(self.ddl_transaction_deferrable_true);
        try std.testing.expect(self.ddl_transaction_deferrable_false);
        try std.testing.expect(self.ddl_advisory_lock);
        try std.testing.expect(self.ddl_advisory_unlock);
        try std.testing.expect(self.unsupported_query_recursive_cte_stream_plan);
        try std.testing.expect(self.unsupported_query_set_operation_plan);
        try std.testing.expect(self.query_calendar_interval_expression);
        try std.testing.expect(self.unsupported_read_recursive_cte_stream_plan);
        try std.testing.expect(self.unsupported_read_duplicate_output_name);
        try std.testing.expect(self.unsupported_read_aggregate_duplicate_output_name);
        try std.testing.expect(self.unsupported_read_set_operation_union);
        try std.testing.expect(self.unsupported_read_set_operation_intersect);
        try std.testing.expect(self.unsupported_read_set_operation_except);
        try std.testing.expect(self.unsupported_read_ordered_set_aggregate_plan);
        try std.testing.expect(self.ddl_temporal_fk_delete_set_null_action);
        try std.testing.expect(self.ddl_temporal_fk_delete_cascade_action);
        try std.testing.expect(self.unsupported_ddl_temporal_fk_update_action);
        try std.testing.expect(self.unsupported_ddl_prepare_recursive_cte_statement);
        try std.testing.expect(self.unsupported_write_recursive_cte_insert);
        try std.testing.expect(self.unsupported_write_recursive_cte_update);
        try std.testing.expect(self.unsupported_write_recursive_cte_delete);
        try std.testing.expect(self.unsupported_write_recursive_cte_merge);
        try std.testing.expect(self.unsupported_ddl_deferrable_unique_constraint);
        try std.testing.expect(self.unsupported_ddl_deferrable_primary_key);
        try std.testing.expect(self.unsupported_ddl_transaction_scoped_search_path);
        try std.testing.expect(self.read_row_lock_nowait);
        try std.testing.expect(self.read_row_lock_share);
        try std.testing.expect(self.read_row_lock_key_share);
        try std.testing.expect(self.query_row_lock_no_key_update);
        try std.testing.expect(self.merge_mutation_typed_plan);
        try std.testing.expect(self.merge_mutation_default_expressions);
        try std.testing.expect(self.unsupported_write_truncate_multi_table);
        try std.testing.expect(self.unsupported_write_truncate_cascade);
        try std.testing.expect(self.truncate_continue_identity);
        try std.testing.expect(self.truncate_restart_identity);
        try std.testing.expect(self.update_source_claim_nowait);
        try std.testing.expect(self.update_source_claim_no_key_update);
        try std.testing.expect(self.unsupported_read_row_lock_target);
        try std.testing.expect(self.unsupported_update_source_row_lock_target);
        try std.testing.expect(self.unsupported_update_joined_source_row_lock_target);
        try std.testing.expect(self.unsupported_merge_mutation_cte);
        try std.testing.expect(self.update_identity_rewrite);
        try std.testing.expect(self.unsupported_update_non_unique_point_selector);
        try std.testing.expect(self.unsupported_delete_non_unique_point_selector);
        try std.testing.expect(self.unsupported_delete_multi_output_subquery_selector);
        try std.testing.expect(self.unsupported_update_joined_multi_output_subquery_selector);
        try std.testing.expect(self.unsupported_delete_joined_multi_output_subquery_selector);
        try std.testing.expect(self.scalar_membership);
        try std.testing.expect(self.boolean_is_predicate);
        try std.testing.expect(self.boolean_is_not_predicate);
        try std.testing.expect(self.boolean_unknown_predicate);
        try std.testing.expect(self.postfix_null_test_predicate);
        try std.testing.expect(self.expression_postfix_null_test_predicate);
        try std.testing.expect(self.json_access_path);
        try std.testing.expect(self.array_access_path);
        try std.testing.expect(self.text_pattern);
        try std.testing.expect(self.query_access_or_predicates);
        try std.testing.expect(self.query_array_overlap_access_or);
        try std.testing.expect(self.query_access_not_predicates);
        try std.testing.expect(self.expression_predicate);
        try std.testing.expect(self.mixed_scalar_expression_or);
        try std.testing.expect(self.expression_order);
        try std.testing.expect(self.query_order_using_operator);
        try std.testing.expect(self.aggregate_order_using_operator);
        try std.testing.expect(self.join_order_using_operator);
        try std.testing.expect(self.lateral_order_using_operator);
        try std.testing.expect(self.window_order_using_operator);
        try std.testing.expect(self.update_source_order_using_operator);
        try std.testing.expect(self.delete_source_order_using_operator);
        try std.testing.expect(self.query_fixed_interval_expression);
        try std.testing.expect(self.query_now_expression);
        try std.testing.expect(self.query_current_timestamp_expression);
        try std.testing.expect(self.query_uuid_generation_expression);
        try std.testing.expect(self.query_uuid_generate_v4_expression);
        try std.testing.expect(self.query_substring_expression);
        try std.testing.expect(self.query_overlay_expression);
        try std.testing.expect(self.query_translate_expression);
        try std.testing.expect(self.query_split_part_expression);
        try std.testing.expect(self.query_strpos_expression);
        try std.testing.expect(self.query_left_right_expression);
        try std.testing.expect(self.query_trim_variant_expression);
        try std.testing.expect(self.query_regexp_replace_expression);
        try std.testing.expect(self.query_regexp_substr_expression);
        try std.testing.expect(self.query_regexp_match_expression);
        try std.testing.expect(self.query_regexp_count_expression);
        try std.testing.expect(self.query_regexp_instr_expression);
        try std.testing.expect(self.query_pad_expression);
        try std.testing.expect(self.query_repeat_expression);
        try std.testing.expect(self.query_reverse_expression);
        try std.testing.expect(self.query_initcap_expression);
        try std.testing.expect(self.query_text_length_expression);
        try std.testing.expect(self.query_bit_length_expression);
        try std.testing.expect(self.query_md5_expression);
        try std.testing.expect(self.query_concat_ws_expression);
        try std.testing.expect(self.query_nullif_expression);
        try std.testing.expect(self.query_extremum_expression);
        try std.testing.expect(self.query_nullable_pagination);
        try std.testing.expect(self.query_json_build_object_expression);
        try std.testing.expect(self.query_to_jsonb_expression);
        try std.testing.expect(self.query_convert_from_jsonb_expression);
        try std.testing.expect(self.query_cardinality_expression);
        try std.testing.expect(self.query_array_position_expression);
        try std.testing.expect(self.query_array_positions_expression);
        try std.testing.expect(self.query_array_element_transform_expression);
        try std.testing.expect(self.query_array_to_string_expression);
        try std.testing.expect(self.query_string_to_array_expression);
        try std.testing.expect(self.query_starts_with_expression);
        try std.testing.expect(self.query_ends_with_expression);
        try std.testing.expect(self.query_ascii_chr_expression);
        try std.testing.expect(self.query_date_trunc_expression);
        try std.testing.expect(self.query_date_bin_expression);
        try std.testing.expect(self.query_typed_datetime_literal_expression);
        try std.testing.expect(self.query_date_part_expression);
        try std.testing.expect(self.query_date_part_epoch_expression);
        try std.testing.expect(self.conflict_date_bin_update);
        try std.testing.expect(self.conflict_typed_datetime_literal_update);
        try std.testing.expect(self.query_nested_case_fold_text_expression);
        try std.testing.expect(self.cte_stream);
        try std.testing.expect(self.cte_query);
        try std.testing.expect(self.cte_aggregate);
        try std.testing.expect(self.cte_window);
        try std.testing.expect(self.catalog_setup_sql);
        try std.testing.expect(self.applied_catalog_plan);
        try std.testing.expect(self.applied_catalog_rebuild);
        try std.testing.expect(self.applied_catalog_validation);
        try std.testing.expect(self.applied_catalog_rewrite);
        try std.testing.expect(self.deterministic_returning_rows > 0);
        try std.testing.expect(self.deterministic_insert_returning_rows);
        try std.testing.expect(self.deterministic_update_returning_rows);
        try std.testing.expect(self.deterministic_delete_returning_rows);
        try std.testing.expect(self.insert_typed_datetime_literal);
        try std.testing.expect(self.returning_all_insert);
        try std.testing.expect(self.returning_all_update);
        try std.testing.expect(self.returning_all_delete);
        try std.testing.expect(self.returning_all_update_source);
        try std.testing.expect(self.returning_all_delete_source);
        try std.testing.expect(self.returning_all_update_joined_source);
        try std.testing.expect(self.returning_all_delete_joined_source);
        try std.testing.expect(self.conflict_do_nothing_returning_all);
        try std.testing.expect(self.conflict_do_update);
        try std.testing.expect(self.conflict_default_update);
        try std.testing.expect(self.conflict_coalesce_existing_update);
        try std.testing.expect(self.conflict_numeric_expression_update);
        try std.testing.expect(self.conflict_case_expression_update);
        try std.testing.expect(self.conflict_uuid_generation_update);
        try std.testing.expect(self.conflict_text_expression_update);
        try std.testing.expect(self.conflict_octet_length_expression_update);
        try std.testing.expect(self.conflict_bit_length_expression_update);
        try std.testing.expect(self.conflict_regexp_replace_expression_update);
        try std.testing.expect(self.conflict_regexp_match_expression_update);
        try std.testing.expect(self.conflict_regexp_count_expression_update);
        try std.testing.expect(self.conflict_regexp_instr_expression_update);
        try std.testing.expect(self.conflict_regexp_substr_expression_update);
        try std.testing.expect(self.conflict_nested_text_expression_update);
        try std.testing.expect(self.conflict_jsonb_update);
        try std.testing.expect(self.conflict_jsonb_concat_update);
        try std.testing.expect(self.conflict_guard_where);
        try std.testing.expect(self.conflict_guard_where_skip);
        try std.testing.expect(self.conflict_returning_expression);
        try std.testing.expect(self.conflict_interval_update);
        try std.testing.expect(self.multi_row_insert);
        try std.testing.expect(self.multi_row_conflict_do_nothing);
        try std.testing.expect(self.multi_row_conflict_do_nothing_duplicate_target);
        try self.expectRowBatchTransformOpCoverage();
        try std.testing.expect(self.point_update_jsonb);
        try std.testing.expect(self.point_update_jsonb_concat);
        try std.testing.expect(self.point_update_array);
        try std.testing.expect(self.point_update_uuid_generation);
        try std.testing.expect(self.point_update_patch_expression);
        try std.testing.expect(self.update_source_claim_skip_locked);
        try std.testing.expect(self.update_source_pagination);
        try std.testing.expect(self.update_source_nullable_pagination);
        try std.testing.expect(self.update_source_boolean_is_not_predicate);
        try std.testing.expect(self.update_source_returning_expression);
        try std.testing.expect(self.point_update_expression_partial_unique_selector);
        try std.testing.expect(self.point_delete_expression_partial_unique_selector);
        try std.testing.expect(self.delete_source_fetch_pagination);
        try std.testing.expect(self.delete_source_nullable_pagination);
        try std.testing.expect(self.delete_source_boolean_unknown_predicate);
        try std.testing.expect(self.delete_source_returning_expression);
        try std.testing.expect(self.joined_source_ordered_pagination);
        try std.testing.expect(self.joined_source_expression_predicate);
        try std.testing.expect(self.joined_source_expression_group);
        try std.testing.expect(self.joined_source_expression_array);
        try std.testing.expect(self.joined_source_returning_expression);
        try std.testing.expect(self.joined_source_returning_source_field);
        try std.testing.expect(self.joined_source_returning_source_expression);
        try std.testing.expect(self.update_joined_source_returning_source_expression);
        try std.testing.expect(self.delete_joined_source_returning_source_expression);
        try std.testing.expect(self.update_joined_source_non_primary_semijoin);
        try std.testing.expect(self.delete_joined_source_non_primary_semijoin);
        try std.testing.expect(self.update_joined_source_correlated_semijoin);
        try std.testing.expect(self.delete_joined_source_correlated_semijoin);
        try std.testing.expect(self.update_joined_source_correlated_filtered_semijoin);
        try std.testing.expect(self.delete_joined_source_correlated_filtered_semijoin);
        try std.testing.expect(self.update_joined_source_semijoin_match_expression);
        try std.testing.expect(self.delete_joined_source_semijoin_match_expression);
        try std.testing.expect(self.update_joined_source_exists_semijoin);
        try std.testing.expect(self.delete_joined_source_exists_semijoin);
        try std.testing.expect(self.update_joined_source_exists_match_expression);
        try std.testing.expect(self.delete_joined_source_exists_match_expression);
        try std.testing.expect(self.update_joined_source_row_value_semijoin);
        try std.testing.expect(self.delete_joined_source_row_value_semijoin);
        try std.testing.expect(self.update_source_patch_expression);
        try std.testing.expect(self.update_source_increment_expression);
        try std.testing.expect(self.update_source_regexp_replace_expression);
        try std.testing.expect(self.update_source_regexp_match_expression);
        try std.testing.expect(self.update_source_regexp_count_expression);
        try std.testing.expect(self.update_source_regexp_instr_expression);
        try std.testing.expect(self.update_source_regexp_substr_expression);
        try std.testing.expect(self.schema_default_primary_named_conflict_target);
        try std.testing.expect(self.schema_custom_primary_named_conflict_target);
        try std.testing.expect(self.schema_unique_conflict_target);
        try std.testing.expect(self.schema_partial_unique_conflict_target);
        try std.testing.expect(self.schema_expression_unique_conflict_target);
        try std.testing.expect(self.schema_mixed_expression_unique_conflict_target);
        try self.expectTemporalRangeColumnDmlCoverage();
        try std.testing.expect(self.unsupported_duplicate_row_batch_target);
        try std.testing.expect(self.unsupported_duplicate_conflict_update_target);
        try std.testing.expect(self.unsupported_invalid_expression_conflict_target);
        try std.testing.expect(self.unsupported_invalid_named_conflict_target);
        try std.testing.expect(self.unsupported_unvalidated_unique_conflict_target);
        try std.testing.expect(self.to_jsonb_value_wrapper);
        try std.testing.expect(self.to_jsonb_dynamic_expression);
        try std.testing.expect(self.update_source_json_set_expression);
        try std.testing.expect(self.ddl_create_table);
        try std.testing.expect(self.ddl_inline_named_column_constraints);
        try std.testing.expect(self.ddl_temporal_table);
        try std.testing.expect(self.ddl_replace_table);
        try std.testing.expect(self.ddl_create_index);
        try std.testing.expect(self.ddl_create_covering_index);
        try std.testing.expect(self.ddl_drop_index);
        try std.testing.expect(self.ddl_drop_table);
        try std.testing.expect(self.ddl_drop_table_cascade);
        try std.testing.expect(self.ddl_alter_table);
        try std.testing.expect(self.ddl_add_column_default_rewrite);
        try std.testing.expect(self.ddl_create_update_policy);
        try std.testing.expect(self.ddl_drop_update_policy);
        try std.testing.expect(self.ddl_add_unvalidated_unique);
        try std.testing.expect(self.ddl_add_unvalidated_fk);
        try std.testing.expect(self.ddl_add_unvalidated_check);
        try std.testing.expect(self.ddl_validate_constraint);
        try std.testing.expect(self.ddl_drop_constraint);
        try std.testing.expect(self.ddl_drop_column);
        try std.testing.expect(self.ddl_alter_column_default);
        try std.testing.expect(self.ddl_drop_column_default);
        try std.testing.expect(self.ddl_alter_column_not_null);
        try std.testing.expect(self.ddl_drop_column_not_null);
        try std.testing.expect(self.ddl_alter_column_type);
        try std.testing.expect(self.ddl_rename_column);
        try std.testing.expect(self.ddl_rename_constraint);
        try std.testing.expect(self.adapter_noop_transaction);
        try std.testing.expect(self.adapter_noop_transaction_commit);
        try std.testing.expect(self.adapter_noop_transaction_rollback);
        try std.testing.expect(self.adapter_noop_session);
        try std.testing.expect(self.adapter_noop_session_probe);
        try std.testing.expect(self.adapter_noop_schema_namespace);
        try std.testing.expect(self.adapter_noop_extension);
        try std.testing.expect(self.ddl_comment_table);
        try std.testing.expect(self.ddl_comment_column);
        try std.testing.expect(self.ddl_comment_index);
        try std.testing.expect(self.ddl_comment_constraint);
        try std.testing.expect(self.session_set_search_path);
        try std.testing.expect(self.session_reset_search_path);
        try std.testing.expect(self.session_show_search_path);
        try std.testing.expect(self.session_discard);
        try std.testing.expect(self.query_distinct_on);
        try std.testing.expect(self.query_cte_chain);
        try std.testing.expect(self.query_cte_structured_access);
        try std.testing.expect(self.query_cte_expression_access);
        try std.testing.expect(self.query_set_operation_order_limit);
        try std.testing.expect(self.read_set_operation_order_limit);
        try std.testing.expect(self.set_operation_fetch_tail);
        try std.testing.expect(self.set_operation_null_pagination_tail);
        try std.testing.expect(self.cte_set_operation_tail);
        try std.testing.expect(self.set_operation_numeric_range_disjoint);
        try std.testing.expect(self.set_operation_expression_numeric_range_disjoint);
        try std.testing.expect(self.aggregate_offset);
        try std.testing.expect(self.aggregate_input_expression);
        try std.testing.expect(self.aggregate_percentile_cont);
        try std.testing.expect(self.aggregate_percentile_disc);
        try std.testing.expect(self.aggregate_octet_length_expression);
        try std.testing.expect(self.aggregate_bit_length_expression);
        try std.testing.expect(self.aggregate_scalar_minmax);
        try std.testing.expect(self.aggregate_group_expression);
        try std.testing.expect(self.aggregate_group_expression_alias);
        try std.testing.expect(self.aggregate_having_expression);
        try std.testing.expect(self.aggregate_having_any);
        try std.testing.expect(self.aggregate_boolean_having_predicate);
        try std.testing.expect(self.aggregate_boolean_is_not_having);
        try std.testing.expect(self.aggregate_filter_expression);
        try std.testing.expect(self.aggregate_filter_groups);
        try std.testing.expect(self.aggregate_boolean_is_not_filter);
        try std.testing.expect(self.aggregate_boolean_unknown_filter);
        try std.testing.expect(self.aggregate_distinct_json_array_expression);
        try std.testing.expect(self.aggregate_distinct_group_projection);
        try std.testing.expect(self.aggregate_cte_expression_access);
        try std.testing.expect(self.join_structured_side_access);
        try std.testing.expect(self.join_on_side_predicate);
        try std.testing.expect(self.join_on_preserved_side_predicate);
        try std.testing.expect(self.join_on_computed_predicate);
        try std.testing.expect(self.join_expression_order);
        try std.testing.expect(self.join_offset);
        try std.testing.expect(self.lateral_structured_side_access);
        try std.testing.expect(self.lateral_subquery_match_expression);
        try std.testing.expect(self.lateral_subquery_match_expression_or);
        try std.testing.expect(self.lateral_subquery_function_match_expression_or);
        try std.testing.expect(self.lateral_subquery_match_expression_not);
        try std.testing.expect(self.lateral_subquery_match_expression_array);
        try std.testing.expect(self.lateral_expression_order);
        try std.testing.expect(self.lateral_right_offset);
        try std.testing.expect(self.window_rich_functions);
        try std.testing.expect(self.window_source_membership);
        try std.testing.expect(self.window_mixed_order);
        try std.testing.expect(self.window_expression_order);
        try std.testing.expect(self.window_boolean_aggregate_functions);
        try std.testing.expect(self.window_cte);
        try std.testing.expect(self.window_cte_expression_access);
        try std.testing.expect(self.window_offset);
        try std.testing.expect(self.window_frame_signature);
        try std.testing.expect(self.window_scalar_minmax);
        try self.expectParameterizedCoverage();
    }

    pub fn expectParameterizedCoverage(self: @This()) !void {
        try std.testing.expect(self.parameterized_query);
        try std.testing.expect(self.parameterized_aggregate);
        try std.testing.expect(self.parameterized_join);
        try std.testing.expect(self.parameterized_lateral);
        try std.testing.expect(self.parameterized_window);
        try std.testing.expect(self.parameterized_insert);
        try std.testing.expect(self.parameterized_update);
        try std.testing.expect(self.parameterized_delete);
        try std.testing.expect(self.parameterized_update_source);
        try std.testing.expect(self.parameterized_delete_source);
        try std.testing.expect(self.parameterized_update_joined_source);
        try std.testing.expect(self.parameterized_delete_joined_source);
    }

    pub fn expectComputedPatternCoverage(self: @This()) !void {
        try std.testing.expect(self.query_computed_pattern_predicate);
        try std.testing.expect(self.aggregate_computed_pattern_filter);
        try std.testing.expect(self.join_computed_pattern_side_filter);
        try std.testing.expect(self.lateral_computed_pattern_side_filter);
        try std.testing.expect(self.window_computed_pattern_filter);
        try std.testing.expect(self.joined_source_computed_pattern_filter);
    }

    pub fn expectModuloCoverage(self: @This()) !void {
        try std.testing.expect(self.query_modulo_expression);
        try std.testing.expect(self.aggregate_modulo_expression);
        try std.testing.expect(self.window_modulo_expression);
        try std.testing.expect(self.update_source_modulo_expression);
        try std.testing.expect(self.update_joined_source_modulo_expression);
    }

    pub fn expectRegexpCoverage(self: @This()) !void {
        try std.testing.expect(self.query_regexp_replace_expression);
        try std.testing.expect(self.query_regexp_substr_expression);
        try std.testing.expect(self.query_regexp_match_expression);
        try std.testing.expect(self.query_regexp_count_expression);
        try std.testing.expect(self.query_regexp_instr_expression);
        try std.testing.expect(self.aggregate_regexp_numeric_expression);
        try std.testing.expect(self.aggregate_regexp_text_expression);
        try std.testing.expect(self.conflict_regexp_replace_expression_update);
        try std.testing.expect(self.conflict_regexp_match_expression_update);
        try std.testing.expect(self.conflict_regexp_count_expression_update);
        try std.testing.expect(self.conflict_regexp_instr_expression_update);
        try std.testing.expect(self.conflict_regexp_substr_expression_update);
        try std.testing.expect(self.insert_source_conflict_regexp_expression);
        try std.testing.expect(self.update_source_regexp_replace_expression);
        try std.testing.expect(self.update_source_regexp_match_expression);
        try std.testing.expect(self.update_source_regexp_count_expression);
        try std.testing.expect(self.update_source_regexp_instr_expression);
        try std.testing.expect(self.update_source_regexp_substr_expression);
        try std.testing.expect(self.update_joined_source_regexp_expression);
        try std.testing.expect(self.delete_joined_source_regexp_expression);
    }

    pub fn expectArrayCoverage(self: @This()) !void {
        try std.testing.expect(self.query_cardinality_expression);
        try std.testing.expect(self.query_array_position_expression);
        try std.testing.expect(self.query_array_positions_expression);
        try std.testing.expect(self.query_array_element_transform_expression);
        try std.testing.expect(self.query_array_to_string_expression);
        try std.testing.expect(self.query_string_to_array_expression);
        try std.testing.expect(self.point_update_array);
        try std.testing.expect(self.update_joined_source_array_expression);
        try std.testing.expect(self.delete_joined_source_array_expression);
    }

    pub fn expectRowBatchTransformOpCoverage(self: @This()) !void {
        try std.testing.expect(self.write_plan_insert_op_set);
        try std.testing.expect(self.write_plan_insert_op_inc);
        try std.testing.expect(self.write_plan_update_op_set);
        try std.testing.expect(self.write_plan_update_op_push);
        try std.testing.expect(self.write_plan_update_op_pull);
    }

    pub fn expectRowAssignmentCoverage(self: @This()) !void {
        try std.testing.expect(self.conflict_row_assignment);
        try std.testing.expect(self.conflict_row_assignment_default);
        try std.testing.expect(self.conflict_row_assignment_constructor);
        try std.testing.expect(self.update_source_row_assignment);
        try std.testing.expect(self.update_source_row_assignment_default);
        try std.testing.expect(self.update_source_row_assignment_constructor);
        try std.testing.expect(self.update_joined_source_row_assignment);
        try std.testing.expect(self.update_joined_source_row_assignment_default);
        try std.testing.expect(self.update_joined_source_row_assignment_constructor);
    }

    pub fn expectBooleanAssignmentCoverage(self: @This()) !void {
        try std.testing.expect(self.conflict_boolean_expression_update);
        try std.testing.expect(self.update_source_boolean_expression_update);
        try std.testing.expect(self.update_joined_source_boolean_expression_update);
    }

    pub fn expectJsonSetCoverage(self: @This()) !void {
        try std.testing.expect(self.insert_source_conflict_json_set_expression);
        try std.testing.expect(self.update_source_json_set_expression);
        try std.testing.expect(self.update_joined_source_json_set_expression);
    }

    pub fn expectJsonExpressionCoverage(self: @This()) !void {
        try std.testing.expect(self.query_json_build_object_expression);
        try std.testing.expect(self.query_to_jsonb_expression);
        try std.testing.expect(self.to_jsonb_value_wrapper);
        try std.testing.expect(self.to_jsonb_dynamic_expression);
        try std.testing.expect(self.update_joined_source_json_expression);
        try std.testing.expect(self.delete_joined_source_json_expression);
    }

    pub fn expectConflictBooleanGuardCoverage(self: @This()) !void {
        try std.testing.expect(self.insert_source_conflict_boolean_is_not_guard);
    }

    pub fn expectWindowAggregateFilter(self: @This()) !void {
        try std.testing.expect(self.window_aggregate_filter);
    }

    pub fn expectMixedIntervalExpression(self: @This()) !void {
        try std.testing.expect(self.query_mixed_interval_expression);
        try std.testing.expect(self.conflict_mixed_interval_update);
    }

    pub fn expectCurrentTimestampPrecision(self: @This()) !void {
        try std.testing.expect(self.query_current_timestamp_precision_expression);
        try std.testing.expect(self.conflict_current_timestamp_precision);
    }

    pub fn expectCurrentDateExpression(self: @This()) !void {
        try std.testing.expect(self.query_current_date_expression);
        try std.testing.expect(self.conflict_current_date_update);
    }

    pub fn expectTemporalRangeColumnDmlCoverage(self: @This()) !void {
        try std.testing.expect(self.schema_temporal_numrange_insert);
        try std.testing.expect(self.schema_temporal_daterange_insert);
        try std.testing.expect(self.schema_temporal_open_daterange_insert);
        try std.testing.expect(self.schema_temporal_lower_open_daterange_insert);
        try std.testing.expect(self.schema_temporal_numrange_constructor_insert);
        try std.testing.expect(self.schema_temporal_daterange_constructor_insert);
        try std.testing.expect(self.schema_temporal_inclusive_daterange_constructor_insert);
        try std.testing.expect(self.schema_temporal_inclusive_daterange_literal_insert);
        try std.testing.expect(self.schema_temporal_lower_exclusive_daterange_constructor_insert);
        try std.testing.expect(self.schema_temporal_lower_exclusive_daterange_literal_insert);
        try std.testing.expect(self.schema_temporal_tsrange_insert);
        try std.testing.expect(self.schema_temporal_tsrange_constructor_insert);
        try std.testing.expect(self.schema_temporal_tstzrange_insert);
        try std.testing.expect(self.schema_temporal_tstzrange_constructor_insert);
        try std.testing.expect(self.schema_temporal_range_bound_query);
        try std.testing.expect(self.schema_temporal_range_contains_query);
        try std.testing.expect(self.schema_temporal_range_overlap_query);
        try std.testing.expect(self.schema_temporal_inclusive_daterange_overlap_query);
        try std.testing.expect(self.schema_temporal_unique_conflict_upsert);
        try std.testing.expect(self.schema_temporal_fk_ddl);
        try std.testing.expect(self.schema_temporal_portion_update);
        try std.testing.expect(self.schema_temporal_portion_delete);
        try std.testing.expect(self.schema_temporal_range_column_portion_update);
        try std.testing.expect(self.schema_temporal_range_column_portion_delete);
        try std.testing.expect(self.schema_nulls_not_distinct_unique);
        try std.testing.expect(self.unsupported_ddl_system_time_temporal_table);
    }
};

test "sql adapter corpus validates fixture metadata core policy" {
    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(.{
        .name = "missing table summary",
        .sql = "SELECT id FROM usage_records",
        .family = .query,
        .plan = "query:table=usage_records:select=1",
    }));

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(.{
        .name = "unsupported without reason",
        .sql = "SELECT id FROM usage_records FOR SHARE",
        .family = .unsupported_read,
        .plan = "unsupported:read:requires=lock_mode",
    }));

    try validateFixtureMetadataCore(.{
        .name = "valid query",
        .sql = "SELECT id FROM usage_records WHERE tenant_id = $1",
        .family = .query,
        .summary = .{ .table_name = "usage_records", .predicates = 1, .select = 1 },
        .plan = "query:table=usage_records:pred=1:select=1",
        .params = &.{.{ .string = "tenant-a" }},
    });

    try std.testing.expectError(error.TestUnexpectedResult, validateFixtureMetadataCore(.{
        .name = "returning rows on source write",
        .sql = "INSERT INTO usage_records SELECT * FROM staged RETURNING id",
        .family = .insert_source,
        .summary = .{ .table_name = "usage_records", .returning = 1 },
        .plan = "insert_source:table=usage_records:source_table=staged:assignments=1:returning=1:returning_expr=0:returning_all=0",
        .returning_rows = &.{"{\"id\":\"u1\"}"},
    }));
}

test "sql adapter corpus owns ddl applied-plan fixture policy" {
    try std.testing.expect(try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create table",
        .sql = "CREATE TABLE usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records" },
        .plan = "ddl:create_table:table=usage_records:columns=1:if_not_exists=false:replace=false",
    }));
    try std.testing.expect(!try corpusDdlFixtureAppliesFromEmptyCatalog(.{
        .name = "create table if not exists",
        .sql = "CREATE TABLE IF NOT EXISTS usage_records (id text PRIMARY KEY)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records" },
        .plan = "ddl:create_table:table=usage_records:columns=1:if_not_exists=true:replace=false",
    }));
    try std.testing.expect(try corpusDdlFixtureRequiresAppliedPlan(.{
        .name = "create index",
        .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)",
        .family = .ddl,
        .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records" },
        .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:generated_expr=0:where=0:unique=false:if_not_exists=false",
        .applied_plan = "applied:rebuild=true:validation=true:rewrite=false:building_indexes=1:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0",
    }));
    try std.testing.expect(!try corpusDdlFixtureRequiresAppliedPlan(.{
        .name = "set search path",
        .sql = "SET search_path TO public",
        .family = .ddl,
        .summary = .{ .ddl_tag = .set_search_path },
        .plan = "ddl:session:set_search_path:namespaces=1:local=false",
    }));
}

test "sql adapter corpus placeholder coverage ignores literals and comments" {
    try std.testing.expect(sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $2",
        2,
    ));
    try std.testing.expect(!sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $3",
        3,
    ));
    try std.testing.expect(!sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1abc",
        1,
    ));
    try std.testing.expect(sqlParameterCoverageMatches(
        "SELECT '$1', $$ $2 $$, id FROM usage_records -- $3abc\nWHERE tenant_id = $1",
        1,
    ));
}
