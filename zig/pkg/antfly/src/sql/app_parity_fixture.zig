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

const corpus = @import("corpus.zig");
const tokenized = @import("tokenized.zig");

pub const AppParityFixtureMetadataCallbacks = struct {
    schema_json_from_setup_sql: *const fn (
        std.mem.Allocator,
        []const []const u8,
    ) anyerror![]u8,
    applied_ddl_plan: *const fn (
        std.mem.Allocator,
        []const u8,
        corpus.AppParityCorpusEntry,
        *const tokenized.ParsedSql,
    ) anyerror![]u8,
};

pub const AppParityFixtureGenerationCallbacks = struct {
    applied_ddl_plan: *const fn (
        std.mem.Allocator,
        []const u8,
        corpus.AppParityCorpusEntry,
        *const tokenized.ParsedSql,
    ) anyerror![]u8,
};

pub fn fixtureJsonAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    source_sha256: []const u8,
    entries: []const corpus.AppParityCorpusEntry,
    callbacks: AppParityFixtureGenerationCallbacks,
) ![]u8 {
    var encoded_entries = std.ArrayListUnmanaged(corpus.AppParityFixtureEncodedEntry).empty;
    defer encoded_entries.deinit(alloc);
    var skipped_entries = std.ArrayListUnmanaged([]const u8).empty;
    defer skipped_entries.deinit(alloc);
    var owned_applied_plans = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_applied_plans.items) |applied_plan| alloc.free(applied_plan);
        owned_applied_plans.deinit(alloc);
    }

    for (entries) |entry| {
        var applied_plan = entry.applied_plan;
        if (applied_plan.len == 0 and try corpus.corpusDdlFixtureRequiresAppliedPlan(entry)) {
            var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, entry.sql);
            defer parsed_sql.deinit(alloc);
            const derived_applied_plan = callbacks.applied_ddl_plan(alloc, schema_json, entry, &parsed_sql) catch |err| switch (err) {
                error.InvalidSqlCatalog, error.UnsupportedSqlShape => {
                    try skipped_entries.append(alloc, entry.name);
                    continue;
                },
                else => return err,
            };
            try owned_applied_plans.append(alloc, derived_applied_plan);
            applied_plan = derived_applied_plan;
        }

        try encoded_entries.append(alloc, .{
            .entry = entry,
            .applied_plan = applied_plan,
        });
    }
    return try corpus.fixtureJsonAlloc(alloc, schema_json, source_sha256, entries.len, encoded_entries.items, skipped_entries.items);
}

pub fn maybeCheckOrPromoteFixture(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    source_sha256: []const u8,
    entries: []const corpus.AppParityCorpusEntry,
    callbacks: AppParityFixtureGenerationCallbacks,
) !void {
    const mode = try corpus.fixtureGateModeFromEnvAlloc(alloc);
    defer corpus.freeFixtureGateMode(alloc, mode);
    if (mode == .none) return;

    const encoded = try fixtureJsonAlloc(alloc, schema_json, source_sha256, entries, callbacks);
    defer alloc.free(encoded);
    try corpus.checkOrPromoteFixtureJson(alloc, mode, encoded);
}

fn appParitySetupSqlIsValid(
    alloc: std.mem.Allocator,
    setup_sql: []const []const u8,
    callbacks: AppParityFixtureMetadataCallbacks,
) !bool {
    for (setup_sql) |sql| {
        if (sql.len == 0) return false;
    }
    const schema_json = callbacks.schema_json_from_setup_sql(alloc, setup_sql) catch return false;
    defer alloc.free(schema_json);
    return true;
}

fn appParityFixtureAppliedPlanMatchesDerived(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    entry: corpus.AppParityCorpusEntry,
    parsed_sql: *const tokenized.ParsedSql,
    callbacks: AppParityFixtureMetadataCallbacks,
) !bool {
    if (entry.applied_plan.len == 0) return true;
    const derived = callbacks.applied_ddl_plan(alloc, base_schema_json, entry, parsed_sql) catch |err| switch (err) {
        error.InvalidSqlCatalog,
        error.UnsupportedSqlShape,
        error.InvalidSchemaUpdateRequest,
        => return false,
        else => return err,
    };
    defer alloc.free(derived);
    return std.mem.eql(u8, entry.applied_plan, derived);
}

fn appParityFixtureAllowsDocumentSourceSchema(entry: corpus.AppParityCorpusEntry) bool {
    return (entry.family == .read and corpus.corpusFixtureHasDocumentReadSummary(entry.summary)) or
        ((entry.family == .unsupported_read or entry.family == .unsupported_write) and
            std.mem.startsWith(u8, entry.classification_reason, "document_sql_"));
}

pub fn validateAppParityFixtureMetadataWithBaseSchema(
    alloc: std.mem.Allocator,
    entry: corpus.AppParityCorpusEntry,
    base_schema_json: []const u8,
    seen_names: *std.StringHashMapUnmanaged(void),
    callbacks: AppParityFixtureMetadataCallbacks,
) !void {
    var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, entry.sql) catch |err| {
        if (try corpus.sourceCorpusGeneratedParseFailureEntryAlloc(alloc, entry, err)) return;
        return error.TestUnexpectedResult;
    };
    defer parsed_sql.deinit(alloc);
    try corpus.validateFixtureMetadataCoreParsedSql(entry, &parsed_sql);
    if (!corpus.corpusFixtureSqlParameterCoverageMatchesParsedSql(entry, &parsed_sql)) return error.TestUnexpectedResult;
    var owned_applied_base_schema_json: ?[]u8 = null;
    defer if (owned_applied_base_schema_json) |schema_json| alloc.free(schema_json);
    const applied_base_schema_json = if (base_schema_json.len > 0)
        base_schema_json
    else if (entry.apply_setup_sql.len > 0) blk: {
        owned_applied_base_schema_json = callbacks.schema_json_from_setup_sql(alloc, entry.apply_setup_sql) catch return error.TestUnexpectedResult;
        break :blk owned_applied_base_schema_json.?;
    } else "";
    if (entry.applied_plan.len > 0) {
        if (!(try appParityFixtureAppliedPlanMatchesDerived(alloc, applied_base_schema_json, entry, &parsed_sql, callbacks))) {
            return error.TestUnexpectedResult;
        }
    }
    if (entry.apply_setup_sql.len > 0 and !(try appParitySetupSqlIsValid(alloc, entry.apply_setup_sql, callbacks))) {
        return error.TestUnexpectedResult;
    }
    if (entry.source_schema_json.len > 0) {
        var source_schema_valid = try corpus.fixtureSchemaJsonIsRelationalTableAlloc(alloc, entry.source_schema_json);
        if (!source_schema_valid and appParityFixtureAllowsDocumentSourceSchema(entry)) {
            source_schema_valid = try corpus.fixtureSchemaJsonIsDocumentTableAlloc(alloc, entry.source_schema_json);
        }
        if (!source_schema_valid) return error.TestUnexpectedResult;
    }
    for (entry.catalog_tables) |catalog_table| {
        var catalog_schema_valid = try corpus.fixtureSchemaJsonIsRelationalTableAlloc(alloc, catalog_table.schema_json);
        if (!catalog_schema_valid and corpus.appParityEntryHasDocumentViewMappingCatalog(entry)) {
            catalog_schema_valid = try corpus.fixtureSchemaJsonIsDocumentTableAlloc(alloc, catalog_table.schema_json);
        }
        if (!catalog_schema_valid) {
            return error.TestUnexpectedResult;
        }
        if (entry.summary.table_name) |target_table_name| {
            if (entry.family != .ddl and entry.catalog_tables.len == 1 and std.mem.eql(u8, catalog_table.name, target_table_name)) return error.TestUnexpectedResult;
        }
    }
    if (entry.source_schema_json.len > 0 and entry.family != .insert) {
        const source_table_name = (corpus.appParitySourceTableNameParsedSqlAlloc(alloc, entry, &parsed_sql) catch return error.TestUnexpectedResult) orelse return error.TestUnexpectedResult;
        defer alloc.free(@constCast(source_table_name));
        if (source_table_name.len == 0) return error.TestUnexpectedResult;
        if (appParityFixtureAllowsDocumentSourceSchema(entry) and try corpus.fixtureSchemaJsonIsDocumentTableAlloc(alloc, entry.source_schema_json)) {
            if (entry.summary.table_name) |target_table_name| {
                if (!std.mem.eql(u8, source_table_name, target_table_name)) return error.TestUnexpectedResult;
            }
            return validateAppParityFixtureRowsAndName(alloc, entry, seen_names);
        }
        if (!corpus.corpusFixturePlanMatchesSourceTable(entry, source_table_name)) {
            return error.TestUnexpectedResult;
        }
    }
    return validateAppParityFixtureRowsAndName(alloc, entry, seen_names);
}

fn validateAppParityFixtureRowsAndName(
    alloc: std.mem.Allocator,
    entry: corpus.AppParityCorpusEntry,
    seen_names: *std.StringHashMapUnmanaged(void),
) !void {
    for (entry.returning_rows) |returning_row| {
        if (!(try corpus.fixtureJsonTextIsObjectAlloc(alloc, returning_row))) return error.TestUnexpectedResult;
    }
    if (entry.resolver_row_json.len > 0 and !(try corpus.fixtureJsonTextIsObjectAlloc(alloc, entry.resolver_row_json))) {
        return error.TestUnexpectedResult;
    }
    if (seen_names.contains(entry.name)) return error.TestUnexpectedResult;
    try seen_names.put(alloc, entry.name, {});
}

pub fn validateAppParityFixtureMetadata(
    alloc: std.mem.Allocator,
    entry: corpus.AppParityCorpusEntry,
    seen_names: *std.StringHashMapUnmanaged(void),
    callbacks: AppParityFixtureMetadataCallbacks,
) !void {
    return validateAppParityFixtureMetadataWithBaseSchema(alloc, entry, "", seen_names, callbacks);
}
