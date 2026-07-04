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
const storage_schema = @import("../storage/schema.zig");
const relational_rows = @import("../sql/relational_rows.zig");
const impl = @import("table_schema_impl.zig");

pub const ParsedTableSchema = impl.TableSchema;

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return try impl.parseSchemaUpdateRequest(alloc, body);
}

pub fn parseValidatedTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !ParsedTableSchema {
    return try impl.parseSchema(alloc, schema_json);
}

pub fn validateBatchWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: anytype,
) !void {
    try validateWritesAgainstTableSchema(alloc, schema, writes);
}

pub fn validateWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: anytype,
) !void {
    try impl.validateWritesAgainstSchema(alloc, schema, writes);
}

pub fn deriveRuntimeTableSchema(alloc: std.mem.Allocator, schema: ParsedTableSchema) !storage_schema.TableSchema {
    const embedded_dynamic_template_count = countEmbeddedDynamicTemplates(schema);
    const dynamic_template_count = schema.dynamic_templates.len + embedded_dynamic_template_count;
    var dynamic_templates: []storage_schema.DynamicTemplate = if (dynamic_template_count == 0)
        &[_]storage_schema.DynamicTemplate{}
    else
        try alloc.alloc(storage_schema.DynamicTemplate, dynamic_template_count);
    var initialized: usize = 0;
    errdefer if (dynamic_template_count > 0) {
        for (dynamic_templates[0..initialized]) |template| {
            alloc.free(template.name);
            if (template.match_pattern) |value| alloc.free(value);
            if (template.unmatch_pattern) |value| alloc.free(value);
            if (template.path_match) |value| alloc.free(value);
            if (template.path_unmatch) |value| alloc.free(value);
            if (template.match_mapping_type) |value| alloc.free(value);
            alloc.free(template.mapping.analyzer);
        }
        alloc.free(dynamic_templates);
    };
    const full_text_documents = try deriveRuntimeFullTextDocuments(alloc, schema);
    errdefer freeRuntimeFullTextDocuments(alloc, full_text_documents);

    const relational_columns = try deriveRuntimeRelationalColumns(alloc, schema);
    errdefer freeRuntimeRelationalColumns(alloc, relational_columns);
    const primary_key = try deriveRuntimePrimaryKey(alloc, schema);
    errdefer if (primary_key) |key| freeRuntimePrimaryKey(alloc, key);
    const periods = try deriveRuntimePeriods(alloc, schema);
    errdefer freeRuntimePeriods(alloc, periods);
    const foreign_keys = try deriveRuntimeForeignKeys(alloc, schema);
    errdefer freeRuntimeForeignKeys(alloc, foreign_keys);
    const unique_constraints = try deriveRuntimeUniqueConstraints(alloc, schema);
    errdefer freeRuntimeUniqueConstraints(alloc, unique_constraints);
    const checks = try deriveRuntimeRelationalChecks(alloc, schema);
    errdefer freeRuntimeRelationalChecks(alloc, checks);
    try validateRuntimeRelationalIndexMetadata(relational_columns, unique_constraints);
    const external_base_source = if (schema.external_base_source) |source| try cloneRuntimeExternalBaseSource(alloc, source) else null;
    errdefer if (external_base_source) |source| storage_schema.freeExternalBaseSource(alloc, source);
    const storage_mode: storage_schema.StorageMode = switch (schema.storage_mode) {
        .document => .document,
        .relational => .relational,
    };

    for (schema.dynamic_templates) |template| {
        dynamic_templates[initialized] = try runtimeDynamicTemplateFromParsed(alloc, template, null);
        initialized += 1;
    }
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            try appendEmbeddedRuntimeDynamicTemplates(alloc, &dynamic_templates, &initialized, property.name, property);
        }
    }

    return .{
        .version = schema.version,
        .default_type = try alloc.dupe(u8, if (schema.default_type.len > 0) schema.default_type else "_default"),
        .ttl_duration_ns = schema.ttl_duration_ns,
        .ttl_field = try alloc.dupe(u8, schema.ttl_field),
        .enforce_types = schema.enforce_types,
        .storage_mode = storage_mode,
        .dynamic_templates = dynamic_templates,
        .full_text_documents = full_text_documents,
        .relational_columns = relational_columns,
        .primary_key = primary_key,
        .periods = periods,
        .foreign_keys = foreign_keys,
        .unique_constraints = unique_constraints,
        .checks = checks,
        .external_base_source = external_base_source,
        .system_versioned = schema.system_versioned,
    };
}

fn cloneRuntimeExternalBaseSource(
    alloc: std.mem.Allocator,
    source: storage_schema.ExternalBaseSource,
) !storage_schema.ExternalBaseSource {
    const table_id = try alloc.dupe(u8, source.table_id);
    errdefer alloc.free(table_id);
    const source_uri = try alloc.dupe(u8, source.source_uri);
    errdefer alloc.free(source_uri);
    const credential_ref = if (source.credential_ref) |credential| credential_blk: {
        const ref_id = try alloc.dupe(u8, credential.ref_id);
        errdefer alloc.free(ref_id);
        const scope = try alloc.dupe(u8, credential.scope);
        errdefer alloc.free(scope);
        break :credential_blk storage_schema.ExternalCredentialRef{ .ref_id = ref_id, .scope = scope };
    } else null;
    errdefer if (credential_ref) |credential| {
        alloc.free(credential.ref_id);
        alloc.free(credential.scope);
    };
    const snapshot_mode: storage_schema.ExternalSnapshotMode = switch (source.snapshot_mode) {
        .current => .current,
        .snapshot_id => |snapshot_id| .{ .snapshot_id = try alloc.dupe(u8, snapshot_id) },
        .object_version_digest => |digest| .{ .object_version_digest = try alloc.dupe(u8, digest) },
    };
    errdefer switch (snapshot_mode) {
        .current => {},
        .snapshot_id => |snapshot_id| alloc.free(snapshot_id),
        .object_version_digest => |digest| alloc.free(digest),
    };
    const schema_fingerprint = try alloc.dupe(u8, source.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    return .{
        .table_id = table_id,
        .format = source.format,
        .source_uri = source_uri,
        .credential_ref = credential_ref,
        .snapshot_mode = snapshot_mode,
        .schema_fingerprint = schema_fingerprint,
        .write_policy = source.write_policy,
    };
}

fn countEmbeddedDynamicTemplates(schema: ParsedTableSchema) usize {
    var count: usize = 0;
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| count += countEmbeddedDynamicTemplatesForProperty(property);
    }
    return count;
}

fn countEmbeddedDynamicTemplatesForProperty(property: impl.DocumentProperty) usize {
    var count = property.embedded_dynamic_templates.len;
    if (property.embedded_schema) |embedded_schema| count += countEmbeddedDynamicTemplatesForProperty(embedded_schema.*);
    if (property.item) |item| count += countEmbeddedDynamicTemplatesForProperty(item.*);
    for (property.properties) |child| count += countEmbeddedDynamicTemplatesForProperty(child);
    return count;
}

fn appendEmbeddedRuntimeDynamicTemplates(
    alloc: std.mem.Allocator,
    dynamic_templates: *[]storage_schema.DynamicTemplate,
    initialized: *usize,
    path: []const u8,
    property: impl.DocumentProperty,
) !void {
    for (property.embedded_dynamic_templates) |template| {
        dynamic_templates.*[initialized.*] = try runtimeDynamicTemplateFromParsed(alloc, template, path);
        initialized.* += 1;
    }
    if (property.embedded_schema) |embedded_schema| {
        try appendEmbeddedRuntimeDynamicTemplates(alloc, dynamic_templates, initialized, path, embedded_schema.*);
    }
    if (property.item) |item| {
        try appendEmbeddedRuntimeDynamicTemplates(alloc, dynamic_templates, initialized, path, item.*);
    }
    for (property.properties) |child| {
        const child_path = try appendPath(alloc, path, child.name);
        defer alloc.free(child_path);
        try appendEmbeddedRuntimeDynamicTemplates(alloc, dynamic_templates, initialized, child_path, child);
    }
}

fn runtimeDynamicTemplateFromParsed(
    alloc: std.mem.Allocator,
    template: impl.DynamicTemplate,
    scope_path: ?[]const u8,
) !storage_schema.DynamicTemplate {
    const field_type = parseRuntimeFieldType(template.field_type orelse "text");
    return .{
        .name = if (scope_path) |scope| try std.fmt.allocPrint(alloc, "{s}.{s}", .{ scope, template.name }) else try alloc.dupe(u8, template.name),
        .match_pattern = if (template.match_pattern) |value| try alloc.dupe(u8, value) else null,
        .unmatch_pattern = if (template.unmatch_pattern) |value| try alloc.dupe(u8, value) else null,
        .path_match = try scopedPatternAlloc(alloc, scope_path, template.path_match, true),
        .path_unmatch = try scopedPatternAlloc(alloc, scope_path, template.path_unmatch, false),
        .match_mapping_type = if (template.match_mapping_type) |value| try alloc.dupe(u8, value) else null,
        .mapping = .{
            .field_type = field_type,
            .do_index = template.do_index orelse true,
            .store = template.store orelse false,
            .doc_values = template.doc_values orelse false,
            .include_in_all = template.include_in_all orelse false,
            .analyzer = try alloc.dupe(u8, template.analyzer orelse defaultDynamicTemplateAnalyzer(field_type)),
        },
    };
}

fn scopedPatternAlloc(
    alloc: std.mem.Allocator,
    scope_path: ?[]const u8,
    pattern: ?[]const u8,
    default_to_scope: bool,
) !?[]u8 {
    const scope = scope_path orelse return if (pattern) |value| try alloc.dupe(u8, value) else null;
    if (pattern) |value| return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ scope, value });
    if (default_to_scope) return try std.fmt.allocPrint(alloc, "{s}.*", .{scope});
    return null;
}

/// Derive the relational typed-column catalog from a parsed table schema. One
/// column per declared top-level property; nested objects/arrays and json-typed
/// fields become `json` columns (indexed as document subtrees); embeddings are
/// skipped. Mirrors schema_capability.relationalColumnType, but emits runtime
/// AntflyType values for the runtime schema consumed by document_mapper.
fn deriveRuntimeRelationalColumns(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.RelationalColumn {
    var columns = std.ArrayListUnmanaged(storage_schema.RelationalColumn).empty;
    errdefer {
        for (columns.items) |column| freeRuntimeRelationalColumn(alloc, column);
        columns.deinit(alloc);
    }

    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            const required = requiredFieldsContain(document_schema.required_fields, property.name);
            try appendRuntimeRelationalColumnForProperty(alloc, &columns, property, property.name, required, false);
            try appendRuntimeRelationalColumnAliasesForPropertyChildren(alloc, &columns, property, property.name, required);
        }
    }

    return try columns.toOwnedSlice(alloc);
}

fn appendRuntimeRelationalColumnAliasesForPropertyChildren(
    alloc: std.mem.Allocator,
    columns: *std.ArrayListUnmanaged(storage_schema.RelationalColumn),
    property: anytype,
    path: []const u8,
    parent_required: bool,
) !void {
    for (property.properties) |child| {
        const child_path = try appendPath(alloc, path, child.name);
        defer alloc.free(child_path);
        const child_required = parent_required and requiredFieldsContain(property.required_fields, child.name);
        if (child.sql_column_name != null) {
            try appendRuntimeRelationalColumnForProperty(alloc, columns, child, child_path, child_required, true);
        }
        try appendRuntimeRelationalColumnAliasesForPropertyChildren(alloc, columns, child, child_path, child_required);
    }
}

fn appendRuntimeRelationalColumnForProperty(
    alloc: std.mem.Allocator,
    columns: *std.ArrayListUnmanaged(storage_schema.RelationalColumn),
    property: anytype,
    path: []const u8,
    required: bool,
    require_column: bool,
) !void {
    const field_type = runtimeRelationalColumnType(property) orelse {
        if (require_column) return error.InvalidSchemaUpdateRequest;
        return;
    };
    const column_name = property.sql_column_name orelse property.name;
    if (runtimeRelationalColumnNameExists(columns.items, column_name)) return error.InvalidSchemaUpdateRequest;
    const nullable = !required;
    if (property.collation != null and !runtimeRelationalPropertySupportsCollation(property, field_type)) return error.InvalidSchemaUpdateRequest;
    try validateRuntimeRelationalDefault(alloc, property.default_value, field_type);
    try validateRuntimeRelationalOnUpdate(property.on_update_value, field_type);
    const name = try alloc.dupe(u8, column_name);
    var name_owned = true;
    errdefer if (name_owned) alloc.free(name);
    const owned_path = try alloc.dupe(u8, path);
    var path_owned = true;
    errdefer if (path_owned) alloc.free(owned_path);
    const index_access_method = runtimeRelationalIndexAccessMethodForProperty(property);
    const index_schema_fingerprint = try runtimeRelationalIndexSchemaFingerprintAlloc(alloc, property);
    var index_schema_fingerprint_owned = true;
    errdefer if (index_schema_fingerprint_owned) if (index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
    var column: storage_schema.RelationalColumn = .{
        .name = name,
        .path = owned_path,
        .field_type = field_type,
        .array_item_type = runtimeRelationalArrayItemType(property),
        .nullable = nullable,
        .collation = if (property.collation) |collation| try alloc.dupe(u8, collation) else null,
        .indexed = if (property.antfly_index) |indexed| indexed else true,
        .index_lifecycle = runtimeRelationalIndexLifecycle(property.index_lifecycle),
        .index_generation = property.index_generation orelse 0,
        .index_access_method = index_access_method,
        .cardinality_proof = property.cardinality_proof,
    };
    name_owned = false;
    path_owned = false;
    var column_owned = true;
    errdefer if (column_owned) freeRuntimeRelationalColumn(alloc, column);
    column.index_name = if (property.index_name) |index_name| try alloc.dupe(u8, index_name) else null;
    column.index_schema_fingerprint = index_schema_fingerprint;
    index_schema_fingerprint_owned = false;
    column.index_include_columns = try cloneStringSlice(alloc, property.index_include_columns);
    column.index_keys = try cloneRelationalIndexKeys(alloc, property.index_keys);
    column.default_value = if (property.default_value) |default_value| try cloneRelationalDefaultValue(alloc, default_value) else null;
    column.on_update_value = if (property.on_update_value) |on_update_value| try cloneRelationalDefaultValue(alloc, on_update_value) else null;
    column.generated = if (property.generated) |generated| try cloneRelationalGeneratedValue(alloc, generated) else null;
    column.index_where = try cloneUniquePredicates(alloc, property.index_where);
    column.index_where_expressions = try cloneRelationalRowsExpressionConditionsAlloc(alloc, property.index_where_expressions);
    try columns.append(alloc, column);
    column_owned = false;
}

fn runtimeRelationalIndexAccessMethodForProperty(property: anytype) ?storage_schema.RelationalIndexAccessMethod {
    if (property.index_access_method) |access_method| return access_method;
    if (property.index_name == null) return null;
    if (property.index_keys.len == 0) return .scalar_column;
    return .ordered_tuple;
}

fn runtimeRelationalIndexSchemaFingerprintAlloc(alloc: std.mem.Allocator, property: anytype) !?[]u8 {
    if (property.index_schema_fingerprint) |fingerprint| return try alloc.dupe(u8, fingerprint);
    const index_name = property.index_name orelse return null;
    return try std.fmt.allocPrint(alloc, "legacy-secondary-index-v1:{s}", .{index_name});
}

fn runtimeRelationalColumnNameExists(columns: []const storage_schema.RelationalColumn, name: []const u8) bool {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return true;
    }
    return false;
}

fn runtimeRelationalColumnForField(columns: []const storage_schema.RelationalColumn, field: []const u8) ?storage_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, field) or std.mem.eql(u8, column.path, field)) return column;
    }
    return null;
}

fn validateRuntimeRelationalIndexMetadata(
    columns: []const storage_schema.RelationalColumn,
    unique_constraints: []const storage_schema.UniqueConstraint,
) !void {
    for (columns) |column| {
        try validateRuntimeRelationalColumnIndexIdentity(columns, column);
        try validateRuntimeRelationalIndexKeys(columns, column.index_keys);
        try validateRuntimeRelationalFieldReferences(columns, column.index_include_columns);
        try validateRuntimeRelationalPredicates(columns, column.index_where);
        try validateRuntimeRelationalExpressionConditions(columns, column.index_where_expressions);
    }
    for (unique_constraints) |constraint| {
        try validateRuntimeRelationalUniqueIndexIdentity(constraint);
        try validateRuntimeRelationalFieldReferences(columns, constraint.columns);
        try validateRuntimeRelationalUniqueExpressions(columns, constraint.expressions);
        try validateRuntimeRelationalFieldReferences(columns, constraint.include_columns);
        try validateRuntimeRelationalIndexKeys(columns, constraint.index_keys);
        try validateRuntimeRelationalPredicates(columns, constraint.where);
        try validateRuntimeRelationalExpressionConditions(columns, constraint.where_expressions);
    }
}

fn validateRuntimeRelationalUniqueIndexIdentity(
    constraint: storage_schema.UniqueConstraint,
) !void {
    if (constraint.index_generation == 0 and constraint.index_access_method == null and constraint.index_schema_fingerprint == null and constraint.index_lifecycle == .ready) return;
    if (constraint.index_generation == 0) return error.InvalidSchemaUpdateRequest;
    const access_method = constraint.index_access_method orelse return error.InvalidSchemaUpdateRequest;
    const fingerprint = constraint.index_schema_fingerprint orelse return error.InvalidSchemaUpdateRequest;
    if (fingerprint.len == 0) return error.InvalidSchemaUpdateRequest;
    switch (access_method) {
        .ordered_tuple => if (constraint.columns.len + constraint.expressions.len == 0) return error.InvalidSchemaUpdateRequest,
        .scalar_column, .algebraic_filter, .text_search => return error.InvalidSchemaUpdateRequest,
    }
}

fn validateRuntimeRelationalColumnIndexIdentity(
    columns: []const storage_schema.RelationalColumn,
    column: storage_schema.RelationalColumn,
) !void {
    const index_name = column.index_name orelse return;
    const access_method = column.index_access_method orelse return error.InvalidSchemaUpdateRequest;
    const fingerprint = column.index_schema_fingerprint orelse return error.InvalidSchemaUpdateRequest;
    if (fingerprint.len == 0) return error.InvalidSchemaUpdateRequest;
    switch (access_method) {
        .ordered_tuple => if (column.index_keys.len == 0) return error.InvalidSchemaUpdateRequest,
        .scalar_column => if (column.index_keys.len != 0) return error.InvalidSchemaUpdateRequest,
        .algebraic_filter, .text_search => {},
    }
    for (columns) |candidate| {
        const candidate_index_name = candidate.index_name orelse continue;
        if (!std.mem.eql(u8, candidate_index_name, index_name)) continue;
        if (candidate.index_access_method == null) return error.InvalidSchemaUpdateRequest;
        if (candidate.index_access_method.? != access_method) return error.InvalidSchemaUpdateRequest;
        if (candidate.index_generation != column.index_generation) return error.InvalidSchemaUpdateRequest;
        const candidate_fingerprint = candidate.index_schema_fingerprint orelse return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, candidate_fingerprint, fingerprint)) return error.InvalidSchemaUpdateRequest;
    }
}

fn validateRuntimeRelationalIndexKeys(
    columns: []const storage_schema.RelationalColumn,
    keys: []const storage_schema.RelationalIndexKey,
) !void {
    for (keys) |key| if (runtimeRelationalColumnForField(columns, key.column) == null) return error.InvalidSchemaUpdateRequest;
}

fn validateRuntimeRelationalFieldReferences(
    columns: []const storage_schema.RelationalColumn,
    fields: []const []const u8,
) !void {
    for (fields) |field| if (runtimeRelationalColumnForField(columns, field) == null) return error.InvalidSchemaUpdateRequest;
}

fn validateRuntimeRelationalPredicates(
    columns: []const storage_schema.RelationalColumn,
    predicates: []const storage_schema.UniquePredicate,
) !void {
    for (predicates) |predicate| if (runtimeRelationalColumnForField(columns, predicate.field) == null) return error.InvalidSchemaUpdateRequest;
}

fn validateRuntimeRelationalUniqueExpressions(
    columns: []const storage_schema.RelationalColumn,
    expressions: []const storage_schema.UniqueExpression,
) !void {
    for (expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (runtimeRelationalColumnForField(columns, expression.field) == null) return error.InvalidSchemaUpdateRequest,
            .expression => if (expression.expression) |row_expression| try validateRuntimeRelationalExpression(columns, row_expression) else return error.InvalidSchemaUpdateRequest,
        }
    }
}

fn validateRuntimeRelationalExpressionConditions(
    columns: []const storage_schema.RelationalColumn,
    conditions: []const storage_schema.RelationalRowsExpressionCondition,
) !void {
    for (conditions) |condition| {
        try validateRuntimeRelationalExpression(columns, condition.lhs);
        for (condition.rhs) |rhs| try validateRuntimeRelationalExpression(columns, rhs);
    }
}

fn validateRuntimeRelationalExpression(
    columns: []const storage_schema.RelationalColumn,
    expression: storage_schema.RelationalRowsExpression,
) !void {
    if (expression.field.len != 0 and expression.field_source == .row and runtimeRelationalColumnForField(columns, expression.field) == null) {
        return error.InvalidSchemaUpdateRequest;
    }
    for (expression.operands) |operand| try validateRuntimeRelationalExpression(columns, operand);
    for (expression.case_branches) |branch| {
        try validateRuntimeRelationalExpression(columns, branch.when.lhs);
        for (branch.when.rhs) |rhs| try validateRuntimeRelationalExpression(columns, rhs);
        try validateRuntimeRelationalExpression(columns, branch.then);
    }
    for (expression.case_else) |fallback| try validateRuntimeRelationalExpression(columns, fallback);
}

fn runtimeRelationalColumnSupportsCollation(field_type: storage_schema.AntflyType) bool {
    return switch (field_type) {
        .keyword, .text => true,
        else => false,
    };
}

fn runtimeRelationalPropertySupportsCollation(property: anytype, field_type: storage_schema.AntflyType) bool {
    if (runtimeRelationalColumnSupportsCollation(field_type)) return true;
    if (field_type != .array) return false;
    const item_type = runtimeRelationalArrayItemType(property) orelse return false;
    return runtimeRelationalColumnSupportsCollation(item_type);
}

fn validateRuntimeRelationalDefault(
    alloc: std.mem.Allocator,
    default_value: ?impl.RelationalDefaultValue,
    field_type: storage_schema.AntflyType,
) !void {
    const value = default_value orelse return;
    switch (value.kind) {
        .literal => try validateRuntimeRelationalLiteralDefault(alloc, value.value_json, field_type),
        .now_ns => switch (field_type) {
            .numeric, .datetime => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .current_date_ns => switch (field_type) {
            .numeric, .datetime => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .uuid_v4 => switch (field_type) {
            .keyword, .text, .link => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .sequence_next => switch (field_type) {
            .numeric => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .scalar_subquery => {},
    }
}

fn validateRuntimeRelationalLiteralDefault(
    alloc: std.mem.Allocator,
    value_json: []const u8,
    field_type: storage_schema.AntflyType,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidSchemaUpdateRequest;
    defer parsed.deinit();
    if (parsed.value == .null) return;
    switch (field_type) {
        .keyword, .text, .link, .blob, .datetime => {
            if (parsed.value != .string) return error.InvalidSchemaUpdateRequest;
        },
        .numeric => switch (parsed.value) {
            .integer, .float => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .boolean => {
            if (parsed.value != .bool) return error.InvalidSchemaUpdateRequest;
        },
        .json => {},
        .array => {
            if (parsed.value != .array) return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validateRuntimeRelationalOnUpdate(on_update_value: ?impl.RelationalDefaultValue, field_type: storage_schema.AntflyType) !void {
    const value = on_update_value orelse return;
    switch (value.kind) {
        .now_ns => switch (field_type) {
            .numeric, .datetime => {},
            else => return error.InvalidSchemaUpdateRequest,
        },
        .literal, .current_date_ns, .uuid_v4, .sequence_next, .scalar_subquery => return error.InvalidSchemaUpdateRequest,
    }
}

fn freeRuntimeRelationalColumns(alloc: std.mem.Allocator, columns: []storage_schema.RelationalColumn) void {
    for (columns) |column| {
        freeRuntimeRelationalColumn(alloc, column);
    }
    if (columns.len > 0) alloc.free(columns);
}

fn freeRuntimeRelationalColumn(alloc: std.mem.Allocator, column: storage_schema.RelationalColumn) void {
    alloc.free(column.name);
    alloc.free(column.path);
    if (column.collation) |collation| alloc.free(collation);
    if (column.index_name) |index_name| alloc.free(index_name);
    if (column.index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
    for (column.index_include_columns) |field_name| alloc.free(field_name);
    if (column.index_include_columns.len > 0) alloc.free(column.index_include_columns);
    storage_schema.freeRelationalIndexKeySlice(alloc, column.index_keys);
    if (column.default_value) |value| alloc.free(value.value_json);
    if (column.on_update_value) |value| alloc.free(value.value_json);
    if (column.generated) |value| freeRuntimeRelationalGeneratedValue(alloc, value);
    for (column.index_where) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (column.index_where.len > 0) alloc.free(column.index_where);
    freeRelationalRowsExpressionConditions(alloc, column.index_where_expressions);
}

fn cloneRelationalDefaultValue(
    alloc: std.mem.Allocator,
    value: impl.RelationalDefaultValue,
) !storage_schema.RelationalDefaultValue {
    const kind: storage_schema.RelationalDefaultKind = switch (value.kind) {
        .literal => .literal,
        .now_ns => .now_ns,
        .current_date_ns => .current_date_ns,
        .uuid_v4 => .uuid_v4,
        .sequence_next => .sequence_next,
        .scalar_subquery => .scalar_subquery,
    };
    return .{
        .kind = kind,
        .value_json = if (kind == .scalar_subquery)
            try relational_rows.normalizeScalarSubqueryDefaultValueJsonAlloc(alloc, value.value_json)
        else
            try alloc.dupe(u8, value.value_json),
    };
}

fn cloneRelationalGeneratedValue(
    alloc: std.mem.Allocator,
    value: impl.RelationalGeneratedValue,
) !storage_schema.RelationalGeneratedValue {
    return .{
        .op = switch (value.op) {
            .lower => .lower,
            .upper => .upper,
            .md5 => .md5,
            .concat => .concat,
            .concat_ws => .concat_ws,
            .expression => .expression,
        },
        .field = if (value.field) |field| try alloc.dupe(u8, field) else null,
        .fields = try cloneStringSlice(alloc, value.fields),
        .separator = try alloc.dupe(u8, value.separator),
        .expression = if (value.expression) |expression| try cloneRelationalRowsExpressionAlloc(alloc, expression) else null,
    };
}

fn freeRuntimeRelationalGeneratedValue(alloc: std.mem.Allocator, value: storage_schema.RelationalGeneratedValue) void {
    if (value.field) |field| alloc.free(field);
    for (value.fields) |field| alloc.free(field);
    if (value.fields.len > 0) alloc.free(value.fields);
    alloc.free(value.separator);
    if (value.expression) |expression| freeRelationalRowsExpression(alloc, expression);
}

fn deriveRuntimePrimaryKey(alloc: std.mem.Allocator, schema: ParsedTableSchema) !?storage_schema.PrimaryKey {
    const primary_key = schema.primary_key orelse return null;
    const name = if (primary_key.name) |key_name| try alloc.dupe(u8, key_name) else null;
    errdefer if (name) |key_name| alloc.free(key_name);
    const columns = try cloneStringSlice(alloc, primary_key.columns);
    errdefer {
        for (columns) |column| alloc.free(column);
        if (columns.len > 0) alloc.free(columns);
    }
    const include_columns = try cloneStringSlice(alloc, primary_key.include_columns);
    errdefer {
        for (include_columns) |column| alloc.free(column);
        if (include_columns.len > 0) alloc.free(include_columns);
    }
    const without_overlaps_period = if (primary_key.without_overlaps_period) |period| try alloc.dupe(u8, period) else null;
    return .{
        .name = name,
        .columns = columns,
        .include_columns = include_columns,
        .without_overlaps_period = without_overlaps_period,
        .deferrable = primary_key.deferrable,
        .timing = switch (primary_key.timing) {
            .immediate => .immediate,
            .deferred => .deferred,
        },
    };
}

fn freeRuntimePrimaryKey(alloc: std.mem.Allocator, primary_key: storage_schema.PrimaryKey) void {
    if (primary_key.name) |name| alloc.free(name);
    for (primary_key.columns) |column| alloc.free(column);
    if (primary_key.columns.len > 0) alloc.free(primary_key.columns);
    for (primary_key.include_columns) |column| alloc.free(column);
    if (primary_key.include_columns.len > 0) alloc.free(primary_key.include_columns);
    if (primary_key.without_overlaps_period) |period| alloc.free(period);
}

fn deriveRuntimePeriods(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.RelationalPeriod {
    if (schema.periods.len == 0) return &.{};
    const periods = try alloc.alloc(storage_schema.RelationalPeriod, schema.periods.len);
    var initialized: usize = 0;
    errdefer {
        for (periods[0..initialized]) |period| {
            alloc.free(period.name);
            alloc.free(period.start_column);
            alloc.free(period.end_column);
        }
        alloc.free(periods);
    }
    for (schema.periods) |period| {
        periods[initialized] = .{
            .name = try alloc.dupe(u8, period.name),
            .start_column = try alloc.dupe(u8, period.start_column),
            .end_column = try alloc.dupe(u8, period.end_column),
            .range_type = period.range_type,
        };
        initialized += 1;
    }
    return periods;
}

fn freeRuntimePeriods(alloc: std.mem.Allocator, periods: []storage_schema.RelationalPeriod) void {
    for (periods) |period| {
        alloc.free(period.name);
        alloc.free(period.start_column);
        alloc.free(period.end_column);
    }
    if (periods.len > 0) alloc.free(periods);
}

fn deriveRuntimeForeignKeys(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.ForeignKey {
    if (schema.foreign_keys.len == 0) return &.{};
    const foreign_keys = try alloc.alloc(storage_schema.ForeignKey, schema.foreign_keys.len);
    var initialized: usize = 0;
    errdefer {
        if (initialized > 0) {
            freeRuntimeForeignKeys(alloc, foreign_keys[0..initialized]);
        } else {
            alloc.free(foreign_keys);
        }
    }
    for (schema.foreign_keys) |foreign_key| {
        foreign_keys[initialized] = .{
            .name = try alloc.dupe(u8, foreign_key.name),
            .child_columns = try cloneStringSlice(alloc, foreign_key.columns),
            .child_period = if (foreign_key.period) |period| try alloc.dupe(u8, period) else null,
            .parent_table = try alloc.dupe(u8, foreign_key.references.table),
            .parent_columns = try cloneStringSlice(alloc, foreign_key.references.columns),
            .parent_period = if (foreign_key.references.period) |period| try alloc.dupe(u8, period) else null,
            .on_delete = switch (foreign_key.on_delete) {
                .restrict => .restrict,
                .no_action => .no_action,
                .set_null => .set_null,
                .cascade => .cascade,
            },
            .on_update = switch (foreign_key.on_update) {
                .restrict => .restrict,
                .no_action => .no_action,
                .set_null => .set_null,
                .cascade => .cascade,
            },
            .timing = switch (foreign_key.timing) {
                .immediate => .immediate,
                .deferred => .deferred,
            },
            .deferrable = foreign_key.deferrable,
            .match = switch (foreign_key.match) {
                .simple => .simple,
                .full => .full,
                .partial => .partial,
            },
            .validation_state = switch (foreign_key.validation_state) {
                .enforced => .enforced,
                .unvalidated => .unvalidated,
                .validating => .validating,
                .invalid => .invalid,
            },
        };
        initialized += 1;
    }
    return foreign_keys;
}

fn freeRuntimeForeignKeys(alloc: std.mem.Allocator, foreign_keys: []storage_schema.ForeignKey) void {
    for (foreign_keys) |foreign_key| {
        alloc.free(foreign_key.name);
        for (foreign_key.child_columns) |column| alloc.free(column);
        if (foreign_key.child_columns.len > 0) alloc.free(foreign_key.child_columns);
        if (foreign_key.child_period) |period| alloc.free(period);
        alloc.free(foreign_key.parent_table);
        for (foreign_key.parent_columns) |column| alloc.free(column);
        if (foreign_key.parent_columns.len > 0) alloc.free(foreign_key.parent_columns);
        if (foreign_key.parent_period) |period| alloc.free(period);
    }
    if (foreign_keys.len > 0) alloc.free(foreign_keys);
}

fn deriveRuntimeUniqueConstraints(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.UniqueConstraint {
    if (schema.unique_constraints.len == 0) return &.{};
    const constraints = try alloc.alloc(storage_schema.UniqueConstraint, schema.unique_constraints.len);
    var initialized: usize = 0;
    errdefer {
        if (initialized > 0) {
            freeRuntimeUniqueConstraints(alloc, constraints[0..initialized]);
        } else {
            alloc.free(constraints);
        }
    }
    for (schema.unique_constraints) |constraint| {
        constraints[initialized] = .{
            .name = try alloc.dupe(u8, constraint.name),
            .columns = try cloneStringSlice(alloc, constraint.columns),
            .expressions = try cloneUniqueExpressions(alloc, constraint.expressions),
            .include_columns = try cloneStringSlice(alloc, constraint.include_columns),
            .index_keys = try cloneRelationalIndexKeys(alloc, constraint.index_keys),
            .index_lifecycle = switch (constraint.index_lifecycle) {
                .ready => .ready,
                .building => .building,
                .invalid => .invalid,
                .dropping => .dropping,
                .catching_up => .catching_up,
                .stale => .stale,
                .rebuild_required => .rebuild_required,
                .failed => .failed,
            },
            .index_generation = constraint.index_generation,
            .index_access_method = constraint.index_access_method,
            .index_schema_fingerprint = if (constraint.index_schema_fingerprint) |fingerprint| try alloc.dupe(u8, fingerprint) else null,
            .without_overlaps_period = if (constraint.without_overlaps_period) |period| try alloc.dupe(u8, period) else null,
            .nulls_not_distinct = constraint.nulls_not_distinct,
            .deferrable = constraint.deferrable,
            .timing = switch (constraint.timing) {
                .immediate => .immediate,
                .deferred => .deferred,
            },
            .where = try cloneUniquePredicates(alloc, constraint.where),
            .where_expressions = try cloneRelationalRowsExpressionConditionsAlloc(alloc, constraint.where_expressions),
            .validation_state = switch (constraint.validation_state) {
                .enforced => .enforced,
                .unvalidated => .unvalidated,
                .validating => .validating,
                .invalid => .invalid,
            },
        };
        initialized += 1;
    }
    return constraints;
}

fn freeRuntimeUniqueConstraints(alloc: std.mem.Allocator, constraints: []storage_schema.UniqueConstraint) void {
    for (constraints) |constraint| {
        alloc.free(constraint.name);
        for (constraint.columns) |column| alloc.free(column);
        if (constraint.columns.len > 0) alloc.free(constraint.columns);
        for (constraint.expressions) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        if (constraint.expressions.len > 0) alloc.free(constraint.expressions);
        for (constraint.include_columns) |column| alloc.free(column);
        if (constraint.include_columns.len > 0) alloc.free(constraint.include_columns);
        storage_schema.freeRelationalIndexKeySlice(alloc, constraint.index_keys);
        if (constraint.index_schema_fingerprint) |fingerprint| alloc.free(fingerprint);
        if (constraint.without_overlaps_period) |period| alloc.free(period);
        for (constraint.where) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        if (constraint.where.len > 0) alloc.free(constraint.where);
        freeRelationalRowsExpressionConditions(alloc, constraint.where_expressions);
    }
    if (constraints.len > 0) alloc.free(constraints);
}

fn deriveRuntimeRelationalChecks(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.RelationalCheck {
    if (schema.checks.len == 0) return &.{};
    const checks = try alloc.alloc(storage_schema.RelationalCheck, schema.checks.len);
    var initialized: usize = 0;
    errdefer {
        if (initialized > 0) {
            freeRuntimeRelationalChecks(alloc, checks[0..initialized]);
        } else {
            alloc.free(checks);
        }
    }
    for (schema.checks) |check| {
        checks[initialized] = .{
            .name = try alloc.dupe(u8, check.name),
            .field = try alloc.dupe(u8, check.field),
            .op = switch (check.op) {
                .is_null => .is_null,
                .is_not_null => .is_not_null,
                .is_distinct => .is_distinct,
                .is_not_distinct => .is_not_distinct,
                .eq => .eq,
                .ne => .ne,
                .gt => .gt,
                .gte => .gte,
                .lt => .lt,
                .lte => .lte,
            },
            .value_json = if (check.value_json) |value_json| try alloc.dupe(u8, value_json) else null,
            .collation = if (check.collation) |collation| try alloc.dupe(u8, collation) else null,
            .validation_state = switch (check.validation_state) {
                .enforced => .enforced,
                .unvalidated => .unvalidated,
                .validating => .validating,
                .invalid => .invalid,
            },
            .expression = if (check.expression) |expression| try cloneRelationalRowsExpressionConditionAlloc(alloc, expression) else null,
        };
        initialized += 1;
    }
    return checks;
}

fn freeRuntimeRelationalChecks(alloc: std.mem.Allocator, checks: []storage_schema.RelationalCheck) void {
    for (checks) |check| {
        alloc.free(check.name);
        alloc.free(check.field);
        if (check.value_json) |value_json| alloc.free(value_json);
        if (check.collation) |collation| alloc.free(collation);
        if (check.expression) |expression| freeRelationalRowsExpressionCondition(alloc, expression);
    }
    if (checks.len > 0) alloc.free(checks);
}

fn cloneRelationalRowsExpressionConditionAlloc(
    alloc: std.mem.Allocator,
    condition: storage_schema.RelationalRowsExpressionCondition,
) anyerror!storage_schema.RelationalRowsExpressionCondition {
    const lhs = try cloneRelationalRowsExpressionAlloc(alloc, condition.lhs);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) freeRelationalRowsExpression(alloc, lhs);
    const rhs = try alloc.alloc(storage_schema.RelationalRowsExpression, condition.rhs.len);
    var rhs_initialized: usize = 0;
    errdefer {
        for (rhs[0..rhs_initialized]) |expression| freeRelationalRowsExpression(alloc, expression);
        if (rhs.len > 0) alloc.free(rhs);
    }
    for (condition.rhs) |expression| {
        rhs[rhs_initialized] = try cloneRelationalRowsExpressionAlloc(alloc, expression);
        rhs_initialized += 1;
    }
    lhs_transferred = true;
    return .{ .lhs = lhs, .op = condition.op, .rhs = rhs };
}

fn cloneRelationalRowsExpressionConditionsAlloc(
    alloc: std.mem.Allocator,
    conditions: []const storage_schema.RelationalRowsExpressionCondition,
) anyerror![]const storage_schema.RelationalRowsExpressionCondition {
    if (conditions.len == 0) return &.{};
    const out = try alloc.alloc(storage_schema.RelationalRowsExpressionCondition, conditions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
        alloc.free(out);
    }
    for (conditions) |condition| {
        out[initialized] = try cloneRelationalRowsExpressionConditionAlloc(alloc, condition);
        initialized += 1;
    }
    return out;
}

fn freeRelationalRowsExpressionConditions(
    alloc: std.mem.Allocator,
    conditions: []const storage_schema.RelationalRowsExpressionCondition,
) void {
    for (conditions) |condition| freeRelationalRowsExpressionCondition(alloc, condition);
    if (conditions.len > 0) alloc.free(conditions);
}

fn cloneRelationalRowsExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: storage_schema.RelationalRowsExpression,
) anyerror!storage_schema.RelationalRowsExpression {
    const field = try alloc.dupe(u8, expression.field);
    var field_transferred = false;
    errdefer if (!field_transferred) alloc.free(field);
    const value_json = try alloc.dupe(u8, expression.value_json);
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const json_path = try alloc.dupe(u8, expression.json_path);
    var path_transferred = false;
    errdefer if (!path_transferred) alloc.free(json_path);

    const operands = try alloc.alloc(storage_schema.RelationalRowsExpression, expression.operands.len);
    var operands_initialized: usize = 0;
    errdefer {
        for (operands[0..operands_initialized]) |operand| freeRelationalRowsExpression(alloc, operand);
        if (operands.len > 0) alloc.free(operands);
    }
    for (expression.operands) |operand| {
        operands[operands_initialized] = try cloneRelationalRowsExpressionAlloc(alloc, operand);
        operands_initialized += 1;
    }

    const branches = try alloc.alloc(storage_schema.RelationalRowsExpressionCaseBranch, expression.case_branches.len);
    var branches_initialized: usize = 0;
    errdefer {
        for (branches[0..branches_initialized]) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
        if (branches.len > 0) alloc.free(branches);
    }
    for (expression.case_branches) |branch| {
        const when = try cloneRelationalRowsExpressionConditionAlloc(alloc, branch.when);
        var when_transferred = false;
        errdefer if (!when_transferred) freeRelationalRowsExpressionCondition(alloc, when);
        const then = try cloneRelationalRowsExpressionAlloc(alloc, branch.then);
        var then_transferred = false;
        errdefer if (!then_transferred) freeRelationalRowsExpression(alloc, then);
        branches[branches_initialized] = .{ .when = when, .then = then };
        when_transferred = true;
        then_transferred = true;
        branches_initialized += 1;
    }

    const fallback = try alloc.alloc(storage_schema.RelationalRowsExpression, expression.case_else.len);
    var fallback_initialized: usize = 0;
    errdefer {
        for (fallback[0..fallback_initialized]) |item| freeRelationalRowsExpression(alloc, item);
        if (fallback.len > 0) alloc.free(fallback);
    }
    for (expression.case_else) |item| {
        fallback[fallback_initialized] = try cloneRelationalRowsExpressionAlloc(alloc, item);
        fallback_initialized += 1;
    }

    field_transferred = true;
    value_transferred = true;
    path_transferred = true;
    return .{
        .kind = expression.kind,
        .field = field,
        .field_source = expression.field_source,
        .value_json = value_json,
        .json_path = json_path,
        .json_as_text = expression.json_as_text,
        .operands = operands,
        .cast_type = expression.cast_type,
        .case_branches = branches,
        .case_else = fallback,
    };
}

fn freeRelationalRowsExpressionCondition(
    alloc: std.mem.Allocator,
    condition: storage_schema.RelationalRowsExpressionCondition,
) void {
    freeRelationalRowsExpression(alloc, condition.lhs);
    for (condition.rhs) |rhs| freeRelationalRowsExpression(alloc, rhs);
    if (condition.rhs.len > 0) alloc.free(condition.rhs);
}

fn freeRelationalRowsExpressionCaseBranch(
    alloc: std.mem.Allocator,
    branch: storage_schema.RelationalRowsExpressionCaseBranch,
) void {
    freeRelationalRowsExpressionCondition(alloc, branch.when);
    freeRelationalRowsExpression(alloc, branch.then);
}

fn freeRelationalRowsExpression(
    alloc: std.mem.Allocator,
    expression: storage_schema.RelationalRowsExpression,
) void {
    alloc.free(expression.field);
    alloc.free(expression.value_json);
    alloc.free(expression.json_path);
    for (expression.operands) |operand| freeRelationalRowsExpression(alloc, operand);
    if (expression.operands.len > 0) alloc.free(expression.operands);
    for (expression.case_branches) |branch| freeRelationalRowsExpressionCaseBranch(alloc, branch);
    if (expression.case_branches.len > 0) alloc.free(expression.case_branches);
    for (expression.case_else) |fallback| freeRelationalRowsExpression(alloc, fallback);
    if (expression.case_else.len > 0) alloc.free(expression.case_else);
}

fn cloneUniqueExpressions(alloc: std.mem.Allocator, values: []const impl.UniqueExpression) ![]const storage_schema.UniqueExpression {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(storage_schema.UniqueExpression, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| {
            alloc.free(expression.field);
            if (expression.expression) |row_expression| freeRelationalRowsExpression(alloc, row_expression);
        }
        alloc.free(out);
    }
    for (values) |value| {
        const field = try alloc.dupe(u8, value.field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        const row_expression = if (value.expression) |expression| try cloneRelationalRowsExpressionAlloc(alloc, expression) else null;
        errdefer if (row_expression) |expression| freeRelationalRowsExpression(alloc, expression);
        out[initialized] = .{
            .op = switch (value.op) {
                .lower => .lower,
                .upper => .upper,
                .md5 => .md5,
                .expression => .expression,
            },
            .field = field,
            .expression = row_expression,
        };
        field_transferred = true;
        initialized += 1;
    }
    return out;
}

fn cloneUniquePredicates(alloc: std.mem.Allocator, values: []const impl.UniquePredicate) ![]const storage_schema.UniquePredicate {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(storage_schema.UniquePredicate, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value_json| alloc.free(value_json);
        }
        alloc.free(out);
    }
    for (values) |value| {
        out[initialized] = .{
            .field = try alloc.dupe(u8, value.field),
            .op = switch (value.op) {
                .is_null => .is_null,
                .is_not_null => .is_not_null,
                .eq => .eq,
                .ne => .ne,
            },
            .value_json = if (value.value_json) |value_json| try alloc.dupe(u8, value_json) else null,
        };
        initialized += 1;
    }
    return out;
}

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values) |value| {
        out[initialized] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn cloneRelationalIndexKeys(alloc: std.mem.Allocator, values: []const storage_schema.RelationalIndexKey) ![]const storage_schema.RelationalIndexKey {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc(storage_schema.RelationalIndexKey, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value.column);
        alloc.free(out);
    }
    for (values) |value| {
        out[initialized] = .{
            .column = try alloc.dupe(u8, value.column),
            .direction = value.direction,
            .nulls = value.nulls,
        };
        initialized += 1;
    }
    return out;
}

fn runtimeRelationalColumnType(property: anytype) ?storage_schema.AntflyType {
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "keyword") or
            std.mem.eql(u8, field_type, "link") or
            std.mem.eql(u8, field_type, "string")) return .keyword;
        if (std.mem.eql(u8, field_type, "text")) return .text;
        if (std.mem.eql(u8, field_type, "html")) return .html;
        if (std.mem.eql(u8, field_type, "search_as_you_type")) return .search_as_you_type;
        if (std.mem.eql(u8, field_type, "boolean")) return .boolean;
        if (std.mem.eql(u8, field_type, "datetime")) return .datetime;
        if (std.mem.eql(u8, field_type, "integer") or
            std.mem.eql(u8, field_type, "numeric") or
            std.mem.eql(u8, field_type, "number")) return .numeric;
        if (std.mem.eql(u8, field_type, "geopoint")) return .geopoint;
        if (std.mem.eql(u8, field_type, "geoshape")) return .geoshape;
        if (std.mem.eql(u8, field_type, "blob")) return .blob;
        if (std.mem.eql(u8, field_type, "array")) return .array;
        if (std.mem.eql(u8, field_type, "json") or
            std.mem.eql(u8, field_type, "object")) return .json;
        if (std.mem.eql(u8, field_type, "embedding")) return null;
        if (property.integer_only) return .numeric;
        return null;
    }
    if (property.integer_only) return .numeric;
    if (property.properties.len > 0 or
        property.item != null or
        (property.additional_properties_allowed orelse false) or
        property.additional_properties_schema != null or
        property.pattern_properties.len > 0 or
        property.dynamic_infer_types) return .json;
    if (property.const_value != null or property.enum_values.len > 0) return .keyword;
    return null;
}

fn runtimeRelationalArrayItemType(property: anytype) ?storage_schema.AntflyType {
    if (property.field_type == null or !std.mem.eql(u8, property.field_type.?, "array")) return null;
    const item = property.item orelse return null;
    return runtimeRelationalColumnType(item.*);
}

fn runtimeRelationalIndexLifecycle(lifecycle: ?impl.RelationalIndexLifecycle) storage_schema.RelationalIndexLifecycle {
    return switch (lifecycle orelse .ready) {
        .ready => .ready,
        .building => .building,
        .invalid => .invalid,
        .dropping => .dropping,
        .catching_up => .catching_up,
        .stale => .stale,
        .rebuild_required => .rebuild_required,
        .failed => .failed,
    };
}

fn requiredFieldsContain(required_fields: []const []const u8, name: []const u8) bool {
    for (required_fields) |field_name| {
        if (std.mem.eql(u8, field_name, name)) return true;
    }
    return false;
}

fn parseRuntimeFieldType(field_type: []const u8) storage_schema.AntflyType {
    if (std.mem.eql(u8, field_type, "text")) return .text;
    if (std.mem.eql(u8, field_type, "keyword")) return .keyword;
    if (std.mem.eql(u8, field_type, "numeric")) return .numeric;
    if (std.mem.eql(u8, field_type, "embedding")) return .embedding;
    if (std.mem.eql(u8, field_type, "link")) return .link;
    if (std.mem.eql(u8, field_type, "boolean")) return .boolean;
    if (std.mem.eql(u8, field_type, "datetime")) return .datetime;
    if (std.mem.eql(u8, field_type, "geopoint")) return .geopoint;
    if (std.mem.eql(u8, field_type, "geoshape")) return .geoshape;
    if (std.mem.eql(u8, field_type, "blob")) return .blob;
    if (std.mem.eql(u8, field_type, "html")) return .html;
    if (std.mem.eql(u8, field_type, "search_as_you_type")) return .search_as_you_type;
    return .text;
}

fn defaultDynamicTemplateAnalyzer(field_type: storage_schema.AntflyType) []const u8 {
    return switch (field_type) {
        .html => "html",
        .keyword, .link => "keyword",
        .search_as_you_type => "search_as_you_type",
        else => "standard",
    };
}

fn freeRuntimeFullTextDocuments(alloc: std.mem.Allocator, docs: []storage_schema.FullTextDocument) void {
    for (docs) |doc| {
        alloc.free(doc.name);
        for (doc.fields) |field| {
            alloc.free(field.path);
            alloc.free(field.emitted_name);
            alloc.free(field.analyzer);
        }
        if (doc.fields.len > 0) alloc.free(doc.fields);
        for (doc.dynamic_rules) |rule| {
            alloc.free(rule.parent_path);
            if (rule.segment_pattern) |pattern| alloc.free(pattern);
            alloc.free(rule.relative_path);
            for (rule.variants) |variant| {
                alloc.free(variant.suffix);
                alloc.free(variant.analyzer);
            }
            if (rule.variants.len > 0) alloc.free(rule.variants);
        }
        if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
        for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
        if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
        for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
        if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
    }
    if (docs.len > 0) alloc.free(docs);
}

fn deriveRuntimeFullTextDocuments(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.FullTextDocument {
    if (schema.document_schemas.len == 0) return &.{};

    const docs = try alloc.alloc(storage_schema.FullTextDocument, schema.document_schemas.len);
    var initialized: usize = 0;
    errdefer {
        for (docs[0..initialized]) |doc| {
            alloc.free(doc.name);
            for (doc.fields) |field| {
                alloc.free(field.path);
                alloc.free(field.emitted_name);
                alloc.free(field.analyzer);
            }
            if (doc.fields.len > 0) alloc.free(doc.fields);
            for (doc.dynamic_rules) |rule| {
                alloc.free(rule.parent_path);
                if (rule.segment_pattern) |pattern| alloc.free(pattern);
                alloc.free(rule.relative_path);
                for (rule.variants) |variant| {
                    alloc.free(variant.suffix);
                    alloc.free(variant.analyzer);
                }
                if (rule.variants.len > 0) alloc.free(rule.variants);
            }
            if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
            for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
            if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
            for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
            if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
        }
        alloc.free(docs);
    }

    for (schema.document_schemas) |document_schema| {
        docs[initialized] = try deriveRuntimeFullTextDocument(alloc, document_schema);
        initialized += 1;
    }
    return docs;
}

fn deriveRuntimeFullTextDocument(
    alloc: std.mem.Allocator,
    document_schema: impl.DocumentSchema,
) !storage_schema.FullTextDocument {
    var fields = std.ArrayListUnmanaged(storage_schema.FullTextField).empty;
    var dynamic_rules = std.ArrayListUnmanaged(storage_schema.FullTextDynamicRule).empty;
    var open_dynamic_paths = std.ArrayListUnmanaged([]const u8).empty;
    var infer_type_dynamic_paths = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| {
            alloc.free(field.path);
            alloc.free(field.emitted_name);
            alloc.free(field.analyzer);
        }
        fields.deinit(alloc);
        for (dynamic_rules.items) |rule| {
            alloc.free(rule.parent_path);
            if (rule.segment_pattern) |pattern| alloc.free(pattern);
            alloc.free(rule.relative_path);
            for (rule.variants) |variant| {
                alloc.free(variant.suffix);
                alloc.free(variant.analyzer);
            }
            if (rule.variants.len > 0) alloc.free(rule.variants);
        }
        dynamic_rules.deinit(alloc);
        for (open_dynamic_paths.items) |open_path| alloc.free(open_path);
        open_dynamic_paths.deinit(alloc);
        for (infer_type_dynamic_paths.items) |infer_path| alloc.free(infer_path);
        infer_type_dynamic_paths.deinit(alloc);
    }

    for (document_schema.properties) |property| {
        try deriveRuntimeFullTextProperty(
            alloc,
            property.name,
            property,
            document_schema.include_in_all_fields,
            &fields,
        );
        try deriveRuntimeFullTextDynamicProperty(alloc, property.name, property, &dynamic_rules);
        try deriveRuntimeFullTextOpenDynamicProperty(alloc, property.name, property, &open_dynamic_paths);
        try deriveRuntimeFullTextInferTypeDynamicProperty(alloc, property.name, property, &infer_type_dynamic_paths);
    }
    for (document_schema.pattern_properties) |pattern_property| {
        try appendDynamicRuleFromProperty(alloc, "", pattern_property.pattern, pattern_property.property.*, &dynamic_rules);
    }
    if (document_schema.additional_properties_schema) |additional_properties| {
        try appendDynamicRuleFromProperty(alloc, "", null, additional_properties.*, &dynamic_rules);
    }
    if (document_schema.dynamic_infer_types and (document_schema.additional_properties_allowed orelse false) and document_schema.additional_properties_schema == null) {
        try appendUniqueOwnedPath(alloc, &infer_type_dynamic_paths, "");
    } else if ((document_schema.additional_properties_allowed orelse false) and document_schema.additional_properties_schema == null) {
        try appendUniqueOwnedPath(alloc, &open_dynamic_paths, "");
    }

    return .{
        .name = try alloc.dupe(u8, document_schema.name),
        .fields = try fields.toOwnedSlice(alloc),
        .dynamic_rules = try dynamic_rules.toOwnedSlice(alloc),
        .open_dynamic_paths = try open_dynamic_paths.toOwnedSlice(alloc),
        .infer_type_dynamic_paths = try infer_type_dynamic_paths.toOwnedSlice(alloc),
    };
}

fn deriveRuntimeFullTextProperty(
    alloc: std.mem.Allocator,
    path: []const u8,
    property: impl.DocumentProperty,
    include_in_all_fields: []const []const u8,
    fields: *std.ArrayListUnmanaged(storage_schema.FullTextField),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.embedded_schema) |embedded_schema| {
        try deriveRuntimeFullTextProperty(alloc, path, embedded_schema.*, embedded_schema.include_in_all_fields, fields);
        return;
    }

    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.properties.len > 0) {
            const child_include = if (item.include_in_all_fields.len > 0) item.include_in_all_fields else property.include_in_all_fields;
            for (item.properties) |child| {
                const child_path = try appendPath(alloc, path, child.name);
                defer alloc.free(child_path);
                try deriveRuntimeFullTextProperty(alloc, child_path, child, child_include, fields);
            }
        } else {
            try deriveRuntimeFullTextLeaf(alloc, path, property, item.*, include_in_all_fields, fields);
        }
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try deriveRuntimeFullTextProperty(alloc, child_path, child, property.include_in_all_fields, fields);
        }
        return;
    }

    try deriveRuntimeFullTextLeaf(alloc, path, property, null, include_in_all_fields, fields);
}

fn deriveRuntimeFullTextLeaf(
    alloc: std.mem.Allocator,
    path: []const u8,
    property: impl.DocumentProperty,
    item: ?impl.DocumentProperty,
    include_in_all_fields: []const []const u8,
    fields: *std.ArrayListUnmanaged(storage_schema.FullTextField),
) !void {
    const types = effectiveAntflyTypes(property, item);
    if (types.len == 0) return;

    const field_name = fieldNameFromPath(path);
    const should_include_in_all = containsString(include_in_all_fields, field_name);
    const primary_analyzer = effectiveAntflyAnalyzer(property, item) orelse "standard";

    const has_text = containsString(types, "text");
    const has_html = containsString(types, "html");
    const has_primary = has_text or has_html;
    const has_keyword = containsString(types, "keyword") or containsString(types, "link");
    const has_search_as_you_type = containsString(types, "search_as_you_type");

    if (has_text and has_html) return;

    if (has_text or (!has_primary and has_search_as_you_type)) {
        try appendFullTextField(alloc, fields, path, path, primary_analyzer, should_include_in_all);
    } else if (has_html) {
        try appendFullTextField(alloc, fields, path, path, effectiveAntflyAnalyzer(property, item) orelse "html", should_include_in_all);
    }

    if (has_keyword) {
        const emitted_name = if (has_primary or has_search_as_you_type)
            try std.fmt.allocPrint(alloc, "{s}.keyword", .{path})
        else
            try alloc.dupe(u8, path);
        defer alloc.free(emitted_name);
        const include = should_include_in_all and !has_primary and !has_search_as_you_type;
        try appendFullTextField(alloc, fields, path, emitted_name, "keyword", include);
    }

    if (has_search_as_you_type) {
        const emitted_2gram = try std.fmt.allocPrint(alloc, "{s}._2gram", .{path});
        defer alloc.free(emitted_2gram);
        try appendFullTextField(alloc, fields, path, emitted_2gram, "search_as_you_type_2gram", false);

        const emitted_3gram = try std.fmt.allocPrint(alloc, "{s}._3gram", .{path});
        defer alloc.free(emitted_3gram);
        try appendFullTextField(alloc, fields, path, emitted_3gram, "search_as_you_type_3gram", false);

        const emitted_index_prefix = try std.fmt.allocPrint(alloc, "{s}._index_prefix", .{path});
        defer alloc.free(emitted_index_prefix);
        try appendFullTextField(alloc, fields, path, emitted_index_prefix, "search_as_you_type_index_prefix", false);
    }
}

fn deriveRuntimeFullTextDynamicProperty(
    alloc: std.mem.Allocator,
    path: []const u8,
    property: impl.DocumentProperty,
    rules: *std.ArrayListUnmanaged(storage_schema.FullTextDynamicRule),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.embedded_schema) |embedded_schema| {
        try deriveRuntimeFullTextDynamicProperty(alloc, path, embedded_schema.*, rules);
        return;
    }

    if (property.additional_properties_schema) |additional_properties| {
        try appendDynamicRuleFromProperty(alloc, path, null, additional_properties.*, rules);
    }
    for (property.pattern_properties) |pattern_property| {
        try appendDynamicRuleFromProperty(alloc, path, pattern_property.pattern, pattern_property.property.*, rules);
    }

    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                const child_path = try appendPath(alloc, path, child.name);
                defer alloc.free(child_path);
                try deriveRuntimeFullTextDynamicProperty(alloc, child_path, child, rules);
            }
        }
        if (item.additional_properties_schema) |additional_properties| {
            try appendDynamicRuleFromProperty(alloc, path, null, additional_properties.*, rules);
        }
        for (item.pattern_properties) |pattern_property| {
            try appendDynamicRuleFromProperty(alloc, path, pattern_property.pattern, pattern_property.property.*, rules);
        }
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try deriveRuntimeFullTextDynamicProperty(alloc, child_path, child, rules);
        }
    }
}

fn deriveRuntimeFullTextOpenDynamicProperty(
    alloc: std.mem.Allocator,
    path: []const u8,
    property: impl.DocumentProperty,
    open_dynamic_paths: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.embedded_schema) |embedded_schema| {
        try deriveRuntimeFullTextOpenDynamicProperty(alloc, path, embedded_schema.*, open_dynamic_paths);
        return;
    }

    if (!property.dynamic_infer_types and (property.additional_properties_allowed orelse false) and property.additional_properties_schema == null) {
        try appendUniqueOwnedPath(alloc, open_dynamic_paths, path);
    }

    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (!item.dynamic_infer_types and (item.additional_properties_allowed orelse false) and item.additional_properties_schema == null) {
            try appendUniqueOwnedPath(alloc, open_dynamic_paths, path);
        }
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                const child_path = try appendPath(alloc, path, child.name);
                defer alloc.free(child_path);
                try deriveRuntimeFullTextOpenDynamicProperty(alloc, child_path, child, open_dynamic_paths);
            }
        }
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try deriveRuntimeFullTextOpenDynamicProperty(alloc, child_path, child, open_dynamic_paths);
        }
    }
}

fn deriveRuntimeFullTextInferTypeDynamicProperty(
    alloc: std.mem.Allocator,
    path: []const u8,
    property: impl.DocumentProperty,
    infer_type_dynamic_paths: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.embedded_schema) |embedded_schema| {
        try deriveRuntimeFullTextInferTypeDynamicProperty(alloc, path, embedded_schema.*, infer_type_dynamic_paths);
        return;
    }

    if (property.dynamic_infer_types and (property.additional_properties_allowed orelse false) and property.additional_properties_schema == null) {
        try appendUniqueOwnedPath(alloc, infer_type_dynamic_paths, path);
    }

    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.dynamic_infer_types and (item.additional_properties_allowed orelse false) and item.additional_properties_schema == null) {
            try appendUniqueOwnedPath(alloc, infer_type_dynamic_paths, path);
        }
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                const child_path = try appendPath(alloc, path, child.name);
                defer alloc.free(child_path);
                try deriveRuntimeFullTextInferTypeDynamicProperty(alloc, child_path, child, infer_type_dynamic_paths);
            }
        }
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try deriveRuntimeFullTextInferTypeDynamicProperty(alloc, child_path, child, infer_type_dynamic_paths);
        }
    }
}

fn appendDynamicRuleFromProperty(
    alloc: std.mem.Allocator,
    parent_path: []const u8,
    segment_pattern: ?[]const u8,
    property: impl.DocumentProperty,
    rules: *std.ArrayListUnmanaged(storage_schema.FullTextDynamicRule),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;
    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                try appendDynamicRuleFromNestedProperty(alloc, parent_path, segment_pattern, child.name, child, rules);
            }
            return;
        }
        return try appendDynamicLeafRule(alloc, parent_path, segment_pattern, "", item.*, rules);
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            try appendDynamicRuleFromNestedProperty(alloc, parent_path, segment_pattern, child.name, child, rules);
        }
        return;
    }

    try appendDynamicLeafRule(alloc, parent_path, segment_pattern, "", property, rules);
}

fn appendDynamicRuleFromNestedProperty(
    alloc: std.mem.Allocator,
    parent_path: []const u8,
    segment_pattern: ?[]const u8,
    relative_path: []const u8,
    property: impl.DocumentProperty,
    rules: *std.ArrayListUnmanaged(storage_schema.FullTextDynamicRule),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;
    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                const child_relative = try appendPath(alloc, relative_path, child.name);
                defer alloc.free(child_relative);
                try appendDynamicRuleFromNestedProperty(alloc, parent_path, segment_pattern, child_relative, child, rules);
            }
            return;
        }
        return try appendDynamicLeafRule(alloc, parent_path, segment_pattern, relative_path, item.*, rules);
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_relative = try appendPath(alloc, relative_path, child.name);
            defer alloc.free(child_relative);
            try appendDynamicRuleFromNestedProperty(alloc, parent_path, segment_pattern, child_relative, child, rules);
        }
        return;
    }

    try appendDynamicLeafRule(alloc, parent_path, segment_pattern, relative_path, property, rules);
}

fn appendDynamicLeafRule(
    alloc: std.mem.Allocator,
    parent_path: []const u8,
    segment_pattern: ?[]const u8,
    relative_path: []const u8,
    property: impl.DocumentProperty,
    rules: *std.ArrayListUnmanaged(storage_schema.FullTextDynamicRule),
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;
    const types = effectiveAntflyTypes(property, null);
    if (types.len == 0) return;

    var variants = std.ArrayListUnmanaged(storage_schema.FullTextDynamicVariant).empty;
    errdefer {
        for (variants.items) |variant| {
            alloc.free(variant.suffix);
            alloc.free(variant.analyzer);
        }
        variants.deinit(alloc);
    }

    const has_text = containsString(types, "text");
    const has_html = containsString(types, "html");
    const has_primary = has_text or has_html;
    const has_keyword = containsString(types, "keyword") or containsString(types, "link");
    const has_search_as_you_type = containsString(types, "search_as_you_type");

    if (has_text and has_html) return;

    if (has_text or (!has_primary and has_search_as_you_type)) {
        try appendDynamicVariant(alloc, &variants, "", "standard", false);
    } else if (has_html) {
        try appendDynamicVariant(alloc, &variants, "", "html", false);
    }

    if (has_keyword) {
        const suffix = if (has_primary or has_search_as_you_type) ".keyword" else "";
        try appendDynamicVariant(alloc, &variants, suffix, "keyword", false);
    }

    if (has_search_as_you_type) {
        try appendDynamicVariant(alloc, &variants, "._2gram", "search_as_you_type_2gram", false);
        try appendDynamicVariant(alloc, &variants, "._3gram", "search_as_you_type_3gram", false);
        try appendDynamicVariant(alloc, &variants, "._index_prefix", "search_as_you_type_index_prefix", false);
    }

    if (variants.items.len == 0) return;
    try rules.append(alloc, .{
        .parent_path = try alloc.dupe(u8, parent_path),
        .segment_pattern = if (segment_pattern) |pattern| try alloc.dupe(u8, pattern) else null,
        .relative_path = try alloc.dupe(u8, relative_path),
        .variants = try variants.toOwnedSlice(alloc),
    });
}

fn appendDynamicVariant(
    alloc: std.mem.Allocator,
    variants: *std.ArrayListUnmanaged(storage_schema.FullTextDynamicVariant),
    suffix: []const u8,
    analyzer: []const u8,
    include_in_all: bool,
) !void {
    try variants.append(alloc, .{
        .suffix = try alloc.dupe(u8, suffix),
        .analyzer = try alloc.dupe(u8, analyzer),
        .include_in_all = include_in_all,
    });
}

fn appendFullTextField(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(storage_schema.FullTextField),
    path: []const u8,
    emitted_name: []const u8,
    analyzer: []const u8,
    include_in_all: bool,
) !void {
    try fields.append(alloc, .{
        .path = try alloc.dupe(u8, path),
        .emitted_name = try alloc.dupe(u8, emitted_name),
        .analyzer = try alloc.dupe(u8, analyzer),
        .include_in_all = include_in_all,
    });
}

fn effectiveAntflyTypes(property: impl.DocumentProperty, item: ?impl.DocumentProperty) []const []const u8 {
    if (property.antfly_types.len > 0) return property.antfly_types;
    if (item) |item_property| {
        if (item_property.antfly_types.len > 0) return item_property.antfly_types;
        if (item_property.field_type) |field_type| {
            if (inferAntflyType(field_type)) |inferred| return inferred;
        }
    }
    if (property.field_type) |field_type| {
        if (inferAntflyType(field_type)) |inferred| return inferred;
    }
    return &.{};
}

fn effectiveAntflyAnalyzer(property: impl.DocumentProperty, item: ?impl.DocumentProperty) ?[]const u8 {
    if (property.analyzer) |analyzer| return analyzer;
    if (item) |item_property| return item_property.analyzer;
    return null;
}

fn inferAntflyType(field_type: []const u8) ?[]const []const u8 {
    if (std.mem.eql(u8, field_type, "string")) return &.{"text"};
    if (std.mem.eql(u8, field_type, "text")) return &.{"text"};
    if (std.mem.eql(u8, field_type, "html")) return &.{"html"};
    if (std.mem.eql(u8, field_type, "keyword")) return &.{"keyword"};
    if (std.mem.eql(u8, field_type, "link")) return &.{"link"};
    if (std.mem.eql(u8, field_type, "search_as_you_type")) return &.{"search_as_you_type"};
    return null;
}

fn appendPath(alloc: std.mem.Allocator, prefix: []const u8, field_name: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, field_name);
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ prefix, field_name });
}

fn containsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn appendUniqueOwnedPath(
    alloc: std.mem.Allocator,
    paths: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (paths.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try paths.append(alloc, try alloc.dupe(u8, value));
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    const idx = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[idx + 1 ..];
}

fn findRuntimeColumn(schema: storage_schema.TableSchema, name: []const u8) ?storage_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

test "deriveRuntimeTableSchema carries nested document SQL column aliases" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{"type":"object","required":["plan"],"properties":{"plan":{"type":"keyword","x-antfly-column-name":"metadata_plan"},"tier":{"type":"keyword"}},"additionalProperties":false},"title":{"type":"text"}},"required":["metadata"],"additionalProperties":false}}}}
    ;

    var parsed = try parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const metadata = findRuntimeColumn(runtime, "metadata") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("metadata", metadata.path);
    try std.testing.expectEqual(storage_schema.AntflyType.json, metadata.field_type);

    const plan = findRuntimeColumn(runtime, "metadata_plan") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("metadata.plan", plan.path);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, plan.field_type);
    try std.testing.expect(!plan.nullable);
    try std.testing.expect(findRuntimeColumn(runtime, "plan") == null);
}

test "deriveRuntimeTableSchema carries document join cardinality proof metadata" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"account_id":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"metadata":{"type":"object","properties":{"plan":{"type":"keyword","x-antfly-column-name":"metadata_plan","x-antfly-cardinality-proof":"unique"}}}}}}}}
    ;

    var parsed = try parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const account_id = findRuntimeColumn(runtime, "account_id") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.RelationalColumnCardinalityProof.unique, account_id.cardinality_proof);
    const metadata_plan = findRuntimeColumn(runtime, "metadata_plan") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("metadata.plan", metadata_plan.path);
    try std.testing.expectEqual(storage_schema.RelationalColumnCardinalityProof.unique, metadata_plan.cardinality_proof);
}

test "deriveRuntimeTableSchema rejects invalid document join cardinality proofs" {
    const alloc = std.testing.allocator;

    const invalid_schemas = [_][]const u8{
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"payload":{"type":"json","x-antfly-cardinality-proof":"unique"}}}}}}
        ,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"account_id":{"type":"keyword","x-antfly-index":false,"x-antfly-cardinality-proof":"unique"}}}}}}
        ,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"account_id":{"type":"keyword","x-antfly-index-where":{"all":[{"field":"account_id","op":"is_not_null"}]},"x-antfly-cardinality-proof":"unique"}}}}}}
        ,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"account_id":{"type":"keyword","x-antfly-index-name":"account_id_idx","x-antfly-index-keys":[{"column":"account_id","direction":"desc"}],"x-antfly-cardinality-proof":"unique"}}}}}}
        ,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{"type":"object","properties":{"plan":{"type":"keyword","x-antfly-cardinality-proof":"unique"}}}}}}}}
        ,
    };

    for (invalid_schemas) |schema_json| {
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc, schema_json));
    }

    var mismatched_fingerprint = try parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword","x-antfly-index-name":"tenant_status_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-generation":7,"x-antfly-index-schema-fingerprint":"secondary-index-v1:left","x-antfly-index-keys":[{"column":"tenant_id"},{"column":"status"}]},"status":{"type":"keyword","x-antfly-index-name":"tenant_status_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-generation":7,"x-antfly-index-schema-fingerprint":"secondary-index-v1:right","x-antfly-index-keys":[{"column":"tenant_id"},{"column":"status"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer mismatched_fingerprint.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, mismatched_fingerprint));
}

test "deriveRuntimeTableSchema rejects invalid or duplicate document SQL column aliases" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"metadata":{"type":"object","properties":{"plan":{"type":"keyword","x-antfly-column-name":"metadata-plan"}}}}}}}
    ));

    var duplicate = try parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"metadata":{"type":"object","properties":{"plan":{"type":"keyword","x-antfly-column-name":"title"}}}}}}}
    );
    defer duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, duplicate));
}

test "deriveRuntimeTableSchema carries external lake base source binding" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"iceberg","uri":"s3://bucket/warehouse/events","credentials":{"ref":"prod-lake-read","scope":"events"},"snapshot":{"mode":"snapshot_id","id":"iceberg-123"},"schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"attrs":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.external_base_source != null);
    try std.testing.expectEqualStrings("events", parsed.external_base_source.?.table_id);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);
    try std.testing.expect(runtime.external_base_source != null);
    try std.testing.expectEqual(storage_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expectEqual(storage_schema.ExternalBaseFormat.iceberg, runtime.external_base_source.?.format);
    try std.testing.expectEqualStrings("events", runtime.external_base_source.?.table_id);
    try std.testing.expectEqualStrings("s3://bucket/warehouse/events", runtime.external_base_source.?.source_uri);
    try std.testing.expect(runtime.external_base_source.?.credential_ref != null);
    try std.testing.expectEqualStrings("prod-lake-read", runtime.external_base_source.?.credential_ref.?.ref_id);
    try std.testing.expectEqualStrings("events", runtime.external_base_source.?.credential_ref.?.scope);
    try std.testing.expectEqualStrings("iceberg-123", runtime.external_base_source.?.snapshot_mode.snapshot_id);
    try std.testing.expectEqualStrings("schema-v5", runtime.external_base_source.?.schema_fingerprint);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","base_source":{"kind":"external","table_id":"events","format":"iceberg","uri":"s3://bucket/warehouse/events","schema_fingerprint":"schema-v1"},"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}}}}}}
    ));
}

test "deriveRuntimeTableSchema carries relational system-versioned marker" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":6,"storage_mode":"relational","default_type":"row","enforce_types":true,"system_versioned":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"valid_from":{"type":"datetime"},"valid_to":{"type":"datetime"}},"required":["id","valid_from","valid_to"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}]}
    );
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.system_versioned);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(storage_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expect(runtime.system_versioned);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","system_versioned":true,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}}}}}}
    ));
}

test "deriveRuntimeTableSchema carries relational storage mode and column catalog" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","collation":"C"},"tenant_id":{"type":"keyword"},"customer_id":{"type":"keyword","x-antfly-index-lifecycle":"building","x-antfly-index-generation":99,"x-antfly-index-name":"customer_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-schema-fingerprint":"secondary-index-v1:customer","x-antfly-index-keys":[{"column":"customer_id","direction":"desc","nulls":"last"},{"column":"tenant_id","direction":"asc","nulls":"first"}],"x-antfly-index-where":{"all":[{"field":"tenant_id","op":"is_not_null"}]}},"amount":{"type":"numeric","x-antfly-index":false},"created_at":{"type":"datetime","x-antfly-on-update":{"op":"now_ns"}},"tags":{"type":"array","items":{"type":"keyword"}},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"payload":{"type":"json"}},"required":["id","tenant_id"],"additionalProperties":false}}},"primary_key":{"name":"orders_pkey","columns":["tenant_id","id"],"include_columns":["created_at","customer_id"]},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","on_update":"no_action"}],"checks":[{"name":"tenant_case_match","field":"tenant_id","op":"eq","value":"TENANT","collation":"antfly.case_insensitive"}]}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(storage_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 8), runtime.relational_columns.len);

    const id = findRuntimeColumn(runtime, "id").?;
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, id.field_type);
    try std.testing.expectEqualStrings("id", id.path);
    try std.testing.expectEqualStrings("C", id.collation.?);
    try std.testing.expect(!id.nullable); // required
    try std.testing.expect(id.indexed);
    const customer_id = findRuntimeColumn(runtime, "customer_id").?;
    try std.testing.expectEqualStrings("customer_idx", customer_id.index_name.?);
    try std.testing.expectEqual(@as(usize, 2), customer_id.index_keys.len);
    try std.testing.expectEqualStrings("customer_id", customer_id.index_keys[0].column);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyDirection.desc, customer_id.index_keys[0].direction);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyNulls.last, customer_id.index_keys[0].nulls);
    try std.testing.expectEqualStrings("tenant_id", customer_id.index_keys[1].column);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyDirection.asc, customer_id.index_keys[1].direction);
    try std.testing.expectEqual(storage_schema.RelationalIndexKeyNulls.first, customer_id.index_keys[1].nulls);
    try std.testing.expectEqual(@as(usize, 1), customer_id.index_where.len);
    try std.testing.expectEqual(storage_schema.RelationalIndexLifecycle.building, customer_id.index_lifecycle);
    try std.testing.expectEqual(@as(u64, 99), customer_id.index_generation);
    try std.testing.expectEqual(storage_schema.RelationalIndexAccessMethod.ordered_tuple, customer_id.index_access_method.?);
    try std.testing.expectEqualStrings("secondary-index-v1:customer", customer_id.index_schema_fingerprint.?);
    try std.testing.expectEqualStrings("tenant_id", customer_id.index_where[0].field);
    try std.testing.expectEqual(storage_schema.UniquePredicateOp.is_not_null, customer_id.index_where[0].op);
    try std.testing.expect(runtime.primary_key != null);
    try std.testing.expectEqualStrings("orders_pkey", runtime.primary_key.?.name.?);
    try std.testing.expectEqual(@as(usize, 2), runtime.primary_key.?.columns.len);
    try std.testing.expectEqualStrings("tenant_id", runtime.primary_key.?.columns[0]);
    try std.testing.expectEqualStrings("id", runtime.primary_key.?.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), runtime.primary_key.?.include_columns.len);
    try std.testing.expectEqualStrings("created_at", runtime.primary_key.?.include_columns[0]);
    try std.testing.expectEqualStrings("customer_id", runtime.primary_key.?.include_columns[1]);
    try std.testing.expectEqual(storage_schema.AntflyType.numeric, findRuntimeColumn(runtime, "amount").?.field_type);
    try std.testing.expect(findRuntimeColumn(runtime, "amount").?.nullable);
    try std.testing.expect(!findRuntimeColumn(runtime, "amount").?.indexed);
    try std.testing.expectEqual(storage_schema.AntflyType.datetime, findRuntimeColumn(runtime, "created_at").?.field_type);
    try std.testing.expect(findRuntimeColumn(runtime, "created_at").?.on_update_value != null);
    try std.testing.expectEqual(storage_schema.RelationalDefaultKind.now_ns, findRuntimeColumn(runtime, "created_at").?.on_update_value.?.kind);
    try std.testing.expectEqual(storage_schema.AntflyType.array, findRuntimeColumn(runtime, "tags").?.field_type);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, findRuntimeColumn(runtime, "tags").?.array_item_type.?);
    // nested object and json field both become json columns
    try std.testing.expectEqual(storage_schema.AntflyType.json, findRuntimeColumn(runtime, "attrs").?.field_type);
    try std.testing.expectEqual(storage_schema.AntflyType.json, findRuntimeColumn(runtime, "payload").?.field_type);
    try std.testing.expectEqual(@as(usize, 1), runtime.foreign_keys.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", runtime.foreign_keys[0].name);
    try std.testing.expectEqualStrings("customer_id", runtime.foreign_keys[0].child_columns[0]);
    try std.testing.expectEqualStrings("customers", runtime.foreign_keys[0].parent_table);
    try std.testing.expectEqualStrings("_id", runtime.foreign_keys[0].parent_columns[0]);
    try std.testing.expectEqual(storage_schema.ForeignKeyAction.no_action, runtime.foreign_keys[0].on_update);
    try std.testing.expectEqual(storage_schema.ForeignKeyMatch.simple, runtime.foreign_keys[0].match);
    try std.testing.expectEqual(@as(usize, 1), runtime.checks.len);
    try std.testing.expectEqualStrings("tenant_case_match", runtime.checks[0].name);
    try std.testing.expectEqualStrings("antfly.case_insensitive", runtime.checks[0].collation.?);
}

test "parseValidatedTableSchema rejects malformed relational index metadata references" {
    const alloc = std.testing.allocator;

    const invalid_schemas = [_][]const u8{
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-index-name":"status_missing_method_idx","x-antfly-index-generation":1,"x-antfly-index-schema-fingerprint":"secondary-index-v1:missing_method","x-antfly-index-keys":[{"column":"status"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-index-name":"status_unknown_method_idx","x-antfly-index-access-method":"btree","x-antfly-index-generation":1,"x-antfly-index-schema-fingerprint":"secondary-index-v1:unknown_method","x-antfly-index-keys":[{"column":"status"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-index-name":"status_missing_fingerprint_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-generation":1,"x-antfly-index-keys":[{"column":"status"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-index-name":"status_missing_key_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-generation":1,"x-antfly-index-schema-fingerprint":"secondary-index-v1:missing_key","x-antfly-index-keys":[{"column":"status"},{"column":"missing"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-index-name":"status_missing_include_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-generation":1,"x-antfly-index-schema-fingerprint":"secondary-index-v1:missing_include","x-antfly-index-keys":[{"column":"status"}],"x-antfly-index-include":["missing"]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword","x-antfly-index-where":{"all":[{"field":"missing","op":"eq","value":"active"}]}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword","x-antfly-index-where-expressions":[{"lhs":{"field":"missing"},"op":"is_not_null"}]}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"email_bad_key","columns":["email"],"index_keys":[{"column":"email"},{"column":"missing"}]}]}
        ,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"email_bad_where_expr","columns":["email"],"where_expressions":[{"lhs":{"op":"lower","args":[{"field":"missing"}]},"op":"eq","rhs":{"value":"active"}}]}]}
        ,
    };

    for (invalid_schemas) |schema_json| {
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc, schema_json));
    }
}

test "deriveRuntimeTableSchema accepts relational index metadata through aliased runtime columns" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword","x-antfly-column-name":"tenant_key"},"status":{"type":"keyword","x-antfly-column-name":"status_key","x-antfly-index-name":"status_tenant_idx","x-antfly-index-access-method":"ordered_tuple","x-antfly-index-schema-fingerprint":"secondary-index-v1:status_tenant","x-antfly-index-generation":1,"x-antfly-index-keys":[{"column":"status"},{"column":"tenant_id"}],"x-antfly-index-include":["tenant_id"],"x-antfly-index-where":{"all":[{"field":"tenant_id","op":"is_not_null"}]},"x-antfly-index-where-expressions":[{"lhs":{"op":"lower","args":[{"field":"tenant_id"}]},"op":"is_not_null"}]}},"required":["id","tenant_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"tenant_status_key","columns":["tenant_id","status"],"index_keys":[{"column":"tenant_id"},{"column":"status"}],"include_columns":["id"],"where":{"all":[{"field":"tenant_id","op":"is_not_null"}]},"where_expressions":[{"lhs":{"field":"status"},"op":"is_not_null"}]}]}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const status = findRuntimeColumn(runtime, "status_key") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("status", status.path);
    try std.testing.expectEqualStrings("status_tenant_idx", status.index_name.?);
    try std.testing.expectEqual(@as(usize, 2), status.index_keys.len);
    try std.testing.expectEqualStrings("status", status.index_keys[0].column);
    try std.testing.expectEqualStrings("tenant_id", status.index_keys[1].column);
    try std.testing.expectEqual(@as(usize, 1), runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("tenant_status_key", runtime.unique_constraints[0].name);
}

test "deriveRuntimeTableSchema rejects relational collation on non text columns" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","collation":"C"}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, parsed));
}

test "deriveRuntimeTableSchema carries relational array item collation" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"},"collation":"antfly.case_insensitive"}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const tags = findRuntimeColumn(runtime, "tags") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(storage_schema.AntflyType.array, tags.field_type);
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, tags.array_item_type.?);
    try std.testing.expectEqualStrings("antfly.case_insensitive", tags.collation.?);
}

test "deriveRuntimeTableSchema validates relational literal default types" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","default":"00000000-0000-0000-0000-000000000000"},"amount":{"type":"numeric","default":0},"enabled":{"type":"boolean","default":true},"tags":{"type":"array","items":{"type":"keyword"},"default":[]},"payload":{"type":"json","default":{"source":"schema"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(storage_schema.AntflyType.numeric, findRuntimeColumn(runtime, "amount").?.field_type);
    try std.testing.expectEqualStrings("0", findRuntimeColumn(runtime, "amount").?.default_value.?.value_json);
    try std.testing.expectEqual(storage_schema.AntflyType.boolean, findRuntimeColumn(runtime, "enabled").?.field_type);
    try std.testing.expectEqualStrings("true", findRuntimeColumn(runtime, "enabled").?.default_value.?.value_json);

    var invalid_numeric = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","default":"0"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer invalid_numeric.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, invalid_numeric));

    var invalid_boolean = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"enabled":{"type":"boolean","default":"true"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer invalid_boolean.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, invalid_boolean));
}

test "deriveRuntimeTableSchema carries sequence-backed relational defaults" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"numeric","x-antfly-default":{"op":"sequence_next","sequence":"usage_id_seq","database":"tenant","schema":"billing"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const id = findRuntimeColumn(runtime, "id").?;
    try std.testing.expect(id.default_value != null);
    try std.testing.expectEqual(storage_schema.RelationalDefaultKind.sequence_next, id.default_value.?.kind);
    try std.testing.expectEqualStrings("{\"sequence\":\"usage_id_seq\",\"database\":\"tenant\",\"schema\":\"billing\"}", id.default_value.?.value_json);

    var invalid_text = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","x-antfly-default":{"op":"sequence_next","sequence":"usage_id_seq"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer invalid_text.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, invalid_text));
}

test "deriveRuntimeTableSchema carries scalar subquery relational defaults" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-default":{"op":"scalar_subquery","query":{"table":"usage_records","select":["status"],"limit":1}}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const status = findRuntimeColumn(runtime, "status").?;
    try std.testing.expect(status.default_value != null);
    try std.testing.expectEqual(storage_schema.RelationalDefaultKind.scalar_subquery, status.default_value.?.kind);
    try std.testing.expectEqualStrings("{\"query\":{\"table\":\"usage_records\",\"select\":[\"status\"],\"limit\":1}}", status.default_value.?.value_json);

    try std.testing.expectError(error.InvalidSchemaUpdateRequest, parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-default":{"op":"scalar_subquery","query":"SELECT status FROM usage_records"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ));
}

test "deriveRuntimeTableSchema normalizes legacy tokenized scalar subquery defaults" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","x-antfly-default":{"op":"scalar_subquery","query":{"kind":"tokenized_sql","tokens":[{"kind":"identifier","text":"SELECT","keyword":"select"},{"kind":"identifier","text":"status"},{"kind":"identifier","text":"FROM","keyword":"from"},{"kind":"identifier","text":"usage_records"},{"kind":"identifier","text":"ORDER","keyword":"order"},{"kind":"identifier","text":"BY","keyword":"by"},{"kind":"identifier","text":"id"},{"kind":"identifier","text":"DESC","keyword":"desc"},{"kind":"identifier","text":"LIMIT","keyword":"limit"},{"kind":"number","text":"1"}]}}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    const status = findRuntimeColumn(runtime, "status").?;
    try std.testing.expect(status.default_value != null);
    try std.testing.expectEqual(storage_schema.RelationalDefaultKind.scalar_subquery, status.default_value.?.kind);
    try std.testing.expectEqualStrings("{\"query\":{\"table\":\"usage_records\",\"select\":[\"status\"],\"order_by\":[{\"field\":\"id\",\"direction\":\"desc\"}],\"limit\":1}}", status.default_value.?.value_json);
}

test "deriveRuntimeTableSchema projects embedded json schema as prefixed document fields" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"title":{"type":"text"},"plan":{"type":"keyword"}},"additionalProperties":true},"dynamic_templates":{"metric":{"path_match":"metrics.*","mapping":{"type":"numeric","doc_values":true}}}}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(storage_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), runtime.full_text_documents.len);
    try std.testing.expect(findRuntimeFullTextField(runtime.full_text_documents[0], "attrs.title") != null);
    try std.testing.expect(findRuntimeFullTextField(runtime.full_text_documents[0], "attrs.plan") != null);
    try std.testing.expectEqual(@as(usize, 1), runtime.full_text_documents[0].open_dynamic_paths.len);
    try std.testing.expectEqualStrings("attrs", runtime.full_text_documents[0].open_dynamic_paths[0]);
    try std.testing.expectEqual(@as(usize, 1), runtime.dynamic_templates.len);
    try std.testing.expectEqualStrings("attrs.metric", runtime.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("attrs.metrics.*", runtime.dynamic_templates[0].path_match.?);
}

test "relational embedded document schema is scoped to explicit json columns" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"attrs":{"type":"json","schema":{"type":"object","properties":{"title":{"type":"text"}},"additionalProperties":true},"dynamic_templates":{"metric":{"path_match":"metrics.*","mapping":{"type":"numeric"}}}}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.document_schemas[0].properties[1].embedded_schema != null);
    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas[0].properties[1].embedded_dynamic_templates.len);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseValidatedTableSchema(alloc,
            \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword","schema":{"type":"object"}}},"required":["id"],"additionalProperties":false}}}}
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseValidatedTableSchema(alloc,
            \\{"version":4,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"attrs":{"type":"object","dynamic_templates":{"metric":{"path_match":"metrics.*","mapping":{"type":"numeric"}}}}},"required":["id"],"additionalProperties":false}}}}
        ),
    );
}

test "relational schema is physically single document schema" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseValidatedTableSchema(alloc,
            \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}},"other":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
        ),
    );
}

test "deriveRuntimeTableSchema defaults to document mode with no relational columns" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"additionalProperties":true}}}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(storage_schema.StorageMode.document, runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), runtime.relational_columns.len);
    try std.testing.expectEqual(storage_schema.AntflyType.text, findRuntimeColumn(runtime, "title").?.field_type);
}

fn findRuntimeFullTextField(document: storage_schema.FullTextDocument, path: []const u8) ?storage_schema.FullTextField {
    for (document.fields) |field| {
        if (std.mem.eql(u8, field.path, path)) return field;
    }
    return null;
}
