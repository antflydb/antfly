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
        for (columns.items) |column| {
            alloc.free(column.name);
            alloc.free(column.path);
        }
        columns.deinit(alloc);
    }

    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            const field_type = runtimeRelationalColumnType(property) orelse continue;
            const nullable = !requiredFieldsContain(document_schema.required_fields, property.name);
            const name = try alloc.dupe(u8, property.name);
            errdefer alloc.free(name);
            const path = try alloc.dupe(u8, property.name);
            errdefer alloc.free(path);
            try columns.append(alloc, .{
                .name = name,
                .path = path,
                .field_type = field_type,
                .nullable = nullable,
            });
        }
    }

    return try columns.toOwnedSlice(alloc);
}

fn freeRuntimeRelationalColumns(alloc: std.mem.Allocator, columns: []storage_schema.RelationalColumn) void {
    for (columns) |column| {
        alloc.free(column.name);
        alloc.free(column.path);
    }
    if (columns.len > 0) alloc.free(columns);
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
        if (std.mem.eql(u8, field_type, "json") or
            std.mem.eql(u8, field_type, "object") or
            std.mem.eql(u8, field_type, "array")) return .json;
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

test "deriveRuntimeTableSchema carries relational storage mode and column catalog" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"payload":{"type":"json"}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(storage_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 5), runtime.relational_columns.len);

    const id = findRuntimeColumn(runtime, "id").?;
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, id.field_type);
    try std.testing.expectEqualStrings("id", id.path);
    try std.testing.expect(!id.nullable); // required
    try std.testing.expectEqual(storage_schema.AntflyType.numeric, findRuntimeColumn(runtime, "amount").?.field_type);
    try std.testing.expect(findRuntimeColumn(runtime, "amount").?.nullable);
    try std.testing.expectEqual(storage_schema.AntflyType.datetime, findRuntimeColumn(runtime, "created_at").?.field_type);
    // nested object and json field both become json columns
    try std.testing.expectEqual(storage_schema.AntflyType.json, findRuntimeColumn(runtime, "attrs").?.field_type);
    try std.testing.expectEqual(storage_schema.AntflyType.json, findRuntimeColumn(runtime, "payload").?.field_type);
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
