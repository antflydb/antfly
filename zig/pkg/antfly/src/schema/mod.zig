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
    const document_field_templates = try deriveRuntimeDocumentFieldTemplates(alloc, schema);
    var document_field_templates_owned = true;
    errdefer if (document_field_templates_owned) freeRuntimeDynamicTemplates(alloc, document_field_templates);

    const dynamic_template_len = schema.dynamic_templates.len + document_field_templates.len;
    var dynamic_templates: []storage_schema.DynamicTemplate = if (dynamic_template_len == 0)
        &[_]storage_schema.DynamicTemplate{}
    else
        try alloc.alloc(storage_schema.DynamicTemplate, dynamic_template_len);
    var initialized: usize = 0;
    errdefer {
        freeRuntimeDynamicTemplateItems(alloc, dynamic_templates[0..initialized]);
        if (dynamic_templates.len > 0) alloc.free(dynamic_templates);
    }
    for (schema.dynamic_templates, 0..) |template, i| {
        dynamic_templates[i] = try runtimeDynamicTemplateFromParsed(alloc, template);
        initialized += 1;
    }
    for (document_field_templates) |template| {
        dynamic_templates[initialized] = template;
        initialized += 1;
    }
    document_field_templates_owned = false;
    if (document_field_templates.len > 0) alloc.free(document_field_templates);

    const full_text_documents = try deriveRuntimeFullTextDocuments(alloc, schema);
    errdefer freeRuntimeFullTextDocuments(alloc, full_text_documents);

    const index_sort = try deriveRuntimeIndexSort(alloc, schema.index_sort, dynamic_templates);
    errdefer freeRuntimeIndexSort(alloc, index_sort);

    return .{
        .version = schema.version,
        .default_type = try alloc.dupe(u8, if (schema.default_type.len > 0) schema.default_type else "_default"),
        .ttl_duration_ns = schema.ttl_duration_ns,
        .ttl_field = try alloc.dupe(u8, schema.ttl_field),
        .enforce_types = schema.enforce_types,
        .dynamic_templates = dynamic_templates,
        .full_text_documents = full_text_documents,
        .index_sort = index_sort,
    };
}

fn freeRuntimeDynamicTemplates(alloc: std.mem.Allocator, templates: []storage_schema.DynamicTemplate) void {
    freeRuntimeDynamicTemplateItems(alloc, templates);
    if (templates.len > 0) alloc.free(templates);
}

fn freeRuntimeDynamicTemplateItems(alloc: std.mem.Allocator, templates: []storage_schema.DynamicTemplate) void {
    for (templates) |template| {
        alloc.free(template.name);
        if (template.match_pattern) |value| alloc.free(value);
        if (template.unmatch_pattern) |value| alloc.free(value);
        if (template.path_match) |value| alloc.free(value);
        if (template.path_unmatch) |value| alloc.free(value);
        if (template.match_mapping_type) |value| alloc.free(value);
        alloc.free(template.mapping.analyzer);
    }
}

fn runtimeDynamicTemplateFromParsed(alloc: std.mem.Allocator, template: impl.DynamicTemplate) !storage_schema.DynamicTemplate {
    const field_type = parseRuntimeFieldType(template.field_type orelse "text");
    const sortable = template.sortable orelse false;
    return .{
        .name = try alloc.dupe(u8, template.name),
        .match_pattern = if (template.match_pattern) |value| try alloc.dupe(u8, value) else null,
        .unmatch_pattern = if (template.unmatch_pattern) |value| try alloc.dupe(u8, value) else null,
        .path_match = if (template.path_match) |value| try alloc.dupe(u8, value) else null,
        .path_unmatch = if (template.path_unmatch) |value| try alloc.dupe(u8, value) else null,
        .match_mapping_type = if (template.match_mapping_type) |value| try alloc.dupe(u8, value) else null,
        .mapping = .{
            .field_type = field_type,
            .do_index = template.do_index orelse true,
            .store = template.store orelse false,
            .doc_values = sortable,
            .sortable = sortable,
            .missing_null_policy = if (template.missing_null_policy) |policy|
                storage_schema.parseMissingNullPolicy(policy) orelse return error.InvalidSchemaUpdateRequest
            else
                .missing_rejected,
            .include_in_all = template.include_in_all orelse false,
            .analyzer = try alloc.dupe(u8, template.analyzer orelse defaultDynamicTemplateAnalyzer(field_type)),
        },
    };
}

fn deriveRuntimeDocumentFieldTemplates(alloc: std.mem.Allocator, schema: ParsedTableSchema) ![]storage_schema.DynamicTemplate {
    var out = std.ArrayListUnmanaged(storage_schema.DynamicTemplate).empty;
    errdefer {
        freeRuntimeDynamicTemplateItems(alloc, out.items);
        out.deinit(alloc);
    }
    for (schema.document_schemas) |document_schema| {
        for (document_schema.properties) |property| {
            try appendRuntimeDocumentFieldTemplates(alloc, &out, property.name, property);
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn appendRuntimeDocumentFieldTemplates(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(storage_schema.DynamicTemplate),
    path: []const u8,
    property: impl.DocumentProperty,
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.item) |item| {
        if (item.antfly_index != null and !item.antfly_index.?) return;
        if (item.properties.len > 0) {
            for (item.properties) |child| {
                const child_path = try appendPath(alloc, path, child.name);
                defer alloc.free(child_path);
                try appendRuntimeDocumentFieldTemplates(alloc, out, child_path, child);
            }
        }
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try appendRuntimeDocumentFieldTemplates(alloc, out, child_path, child);
        }
        return;
    }

    const mapping = property.antfly_field orelse return;
    try appendRuntimeDocumentFieldMappingTemplates(alloc, out, path, mapping);
}

fn appendRuntimeDocumentFieldMappingTemplates(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(storage_schema.DynamicTemplate),
    path: []const u8,
    mapping: impl.DynamicTemplate,
) !void {
    try out.append(alloc, try runtimeDocumentFieldTemplateFromParsed(alloc, path, mapping));
    for (mapping.fields) |subfield| {
        const subfield_path = try appendPath(alloc, path, subfield.name);
        defer alloc.free(subfield_path);
        try out.append(alloc, try runtimeDocumentFieldTemplateFromParsed(alloc, subfield_path, subfield));
    }
}

fn runtimeDocumentFieldTemplateFromParsed(
    alloc: std.mem.Allocator,
    path: []const u8,
    mapping: impl.DynamicTemplate,
) !storage_schema.DynamicTemplate {
    const field_type = parseRuntimeFieldType(mapping.field_type orelse "text");
    const sortable = mapping.sortable orelse false;
    return .{
        .name = try alloc.dupe(u8, path),
        .path_match = try alloc.dupe(u8, path),
        .mapping = .{
            .field_type = field_type,
            .do_index = mapping.do_index orelse true,
            .store = mapping.store orelse false,
            .doc_values = sortable,
            .sortable = sortable,
            .missing_null_policy = if (mapping.missing_null_policy) |policy|
                storage_schema.parseMissingNullPolicy(policy) orelse return error.InvalidSchemaUpdateRequest
            else
                .missing_rejected,
            .include_in_all = mapping.include_in_all orelse false,
            .analyzer = try alloc.dupe(u8, mapping.analyzer orelse defaultDynamicTemplateAnalyzer(field_type)),
        },
    };
}

fn parseRuntimeFieldType(field_type: []const u8) storage_schema.AntflyType {
    if (std.mem.eql(u8, field_type, "text")) return .text;
    if (std.mem.eql(u8, field_type, "keyword")) return .keyword;
    if (std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "integer"))
        return .numeric;
    if (std.mem.eql(u8, field_type, "embedding")) return .embedding;
    if (std.mem.eql(u8, field_type, "link")) return .link;
    if (std.mem.eql(u8, field_type, "boolean") or std.mem.eql(u8, field_type, "bool")) return .boolean;
    if (std.mem.eql(u8, field_type, "datetime") or
        std.mem.eql(u8, field_type, "date") or
        std.mem.eql(u8, field_type, "timestamp"))
        return .datetime;
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

fn freeRuntimeIndexSort(alloc: std.mem.Allocator, fields: []const storage_schema.IndexSortField) void {
    for (fields) |field| alloc.free(field.field);
    if (fields.len > 0) alloc.free(fields);
}

fn deriveRuntimeIndexSort(
    alloc: std.mem.Allocator,
    parsed_fields: []const impl.IndexSortField,
    dynamic_templates: []const storage_schema.DynamicTemplate,
) ![]const storage_schema.IndexSortField {
    if (parsed_fields.len == 0) return &.{};

    var saw_id = false;
    for (parsed_fields, 0..) |field, i| {
        if (std.mem.eql(u8, field.field, "_id")) {
            if (field.desc or i != parsed_fields.len - 1) return error.InvalidSchemaUpdateRequest;
            saw_id = true;
        }
        for (parsed_fields[0..i]) |previous| {
            if (std.mem.eql(u8, previous.field, field.field)) return error.InvalidSchemaUpdateRequest;
        }
    }

    const count = parsed_fields.len + @as(usize, if (saw_id) 0 else 1);
    const fields = try alloc.alloc(storage_schema.IndexSortField, count);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field| alloc.free(field.field);
        alloc.free(fields);
    }

    const validation_schema = storage_schema.TableSchema{
        .dynamic_templates = dynamic_templates,
    };
    for (parsed_fields) |field| {
        if (!std.mem.eql(u8, field.field, "_id")) {
            const mapping = storage_schema.resolveDeclaredFieldType(validation_schema, field.field) orelse return error.InvalidSchemaUpdateRequest;
            if (!mapping.sortable or !mapping.doc_values or !isRuntimeIndexSortScalar(mapping.field_type)) {
                return error.InvalidSchemaUpdateRequest;
            }
        }
        fields[initialized] = .{
            .field = try alloc.dupe(u8, field.field),
            .desc = field.desc,
        };
        initialized += 1;
    }

    if (!saw_id) {
        fields[initialized] = .{
            .field = try alloc.dupe(u8, "_id"),
            .desc = false,
        };
        initialized += 1;
    }
    return fields;
}

fn isRuntimeIndexSortScalar(field_type: storage_schema.AntflyType) bool {
    return switch (field_type) {
        .keyword, .numeric, .boolean, .datetime, .link => true,
        else => false,
    };
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

fn findFieldCapability(capabilities: []const storage_schema.FieldCapability, field: []const u8) ?storage_schema.FieldCapability {
    for (capabilities) |capability| {
        if (capability.field) |capability_field| {
            if (std.mem.eql(u8, capability_field, field)) return capability;
        }
    }
    return null;
}

test "runtime schema derives internal doc values from sortable scalar mappings" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"dates","path_match":"created_at","mapping":{"type":"datetime","sortable":true,"missing_null_policy":"missing_rejected"}},
        \\    {"name":"body","path_match":"body","mapping":{"type":"text"}},
        \\    {"name":"rank","path_match":"rank","mapping":{"type":"numeric","sortable":false}}
        \\  ]
        \\}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(@as(usize, 3), runtime.dynamic_templates.len);
    try std.testing.expect(runtime.dynamic_templates[0].mapping.doc_values);
    try std.testing.expect(runtime.dynamic_templates[0].mapping.sortable);
    try std.testing.expectEqual(storage_schema.MissingNullPolicy.missing_rejected, runtime.dynamic_templates[0].mapping.missing_null_policy);
    try std.testing.expect(!runtime.dynamic_templates[1].mapping.doc_values);
    try std.testing.expect(!runtime.dynamic_templates[1].mapping.sortable);
    try std.testing.expect(!runtime.dynamic_templates[2].mapping.doc_values);
    try std.testing.expect(!runtime.dynamic_templates[2].mapping.sortable);
}

test "runtime schema lowers document field mappings to exact declared fields" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{
        \\  "document_schemas": {
        \\    "doc": {
        \\      "schema": {
        \\        "type": "object",
        \\        "properties": {
        \\          "created_at": {
        \\            "type": "string",
        \\            "format": "date-time",
        \\            "x-antfly-field": {"type":"date","sortable":true}
        \\          },
        \\          "meta": {
        \\            "type": "object",
        \\            "properties": {
        \\              "rank": {"type":"number","x-antfly-field":{"type":"number","sortable":true}}
        \\            }
        \\          },
        \\          "title": {
        \\            "type": "string",
        \\            "x-antfly-field": {
        \\              "type": "text",
        \\              "fields": {
        \\                "keyword": {"type":"keyword","sortable":true}
        \\              }
        \\            }
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(@as(usize, 4), runtime.dynamic_templates.len);
    try std.testing.expectEqualStrings("created_at", runtime.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("created_at", runtime.dynamic_templates[0].path_match.?);
    try std.testing.expectEqual(storage_schema.AntflyType.datetime, runtime.dynamic_templates[0].mapping.field_type);
    try std.testing.expect(runtime.dynamic_templates[0].mapping.doc_values);
    try std.testing.expect(runtime.dynamic_templates[0].mapping.sortable);

    const created_mapping = storage_schema.resolveDeclaredFieldType(runtime, "created_at") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.datetime, created_mapping.field_type);
    try std.testing.expect(created_mapping.doc_values);
    try std.testing.expect(created_mapping.sortable);

    const rank_mapping = storage_schema.resolveDeclaredFieldType(runtime, "meta.rank") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.numeric, rank_mapping.field_type);
    try std.testing.expect(rank_mapping.doc_values);
    try std.testing.expect(rank_mapping.sortable);

    const title_mapping = storage_schema.resolveDeclaredFieldType(runtime, "title") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.text, title_mapping.field_type);
    try std.testing.expect(!title_mapping.doc_values);
    try std.testing.expect(!title_mapping.sortable);

    const keyword_mapping = storage_schema.resolveDeclaredFieldType(runtime, "title.keyword") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, keyword_mapping.field_type);
    try std.testing.expect(keyword_mapping.doc_values);
    try std.testing.expect(keyword_mapping.sortable);

    const capabilities = try storage_schema.fieldCapabilitiesAlloc(alloc, runtime);
    defer storage_schema.freeFieldCapabilities(alloc, capabilities);
    const created_capability = findFieldCapability(capabilities, "created_at") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.datetime, created_capability.field_type);
    try std.testing.expect(created_capability.doc_values);
    try std.testing.expect(created_capability.sortable);
    try std.testing.expectEqualStrings("schema_declared", created_capability.doc_value_coverage);
    try std.testing.expectEqualStrings("dynamic_template", created_capability.provenance);
    try std.testing.expectEqualStrings("declared", created_capability.queryability_state);
    const keyword_capability = findFieldCapability(capabilities, "title.keyword") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(storage_schema.AntflyType.keyword, keyword_capability.field_type);
    try std.testing.expect(keyword_capability.doc_values);
    try std.testing.expect(keyword_capability.sortable);
    try std.testing.expectEqualStrings("schema_declared", keyword_capability.doc_value_coverage);
}

test "runtime schema derives and validates index sort metadata" {
    const alloc = std.testing.allocator;
    var parsed = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}},
        \\    {"name":"rank","path_match":"rank","mapping":{"type":"numeric","sortable":false}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"created_at","order":"desc"}
        \\  ]
        \\}
    );
    defer parsed.deinit(alloc);

    const runtime = try deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    try std.testing.expectEqual(@as(usize, 2), runtime.index_sort.len);
    try std.testing.expectEqualStrings("created_at", runtime.index_sort[0].field);
    try std.testing.expect(runtime.index_sort[0].desc);
    try std.testing.expectEqualStrings("_id", runtime.index_sort[1].field);
    try std.testing.expect(!runtime.index_sort[1].desc);

    var explicit_id = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"created_at","order":"asc"},
        \\    {"field":"_id","order":"asc"}
        \\  ]
        \\}
    );
    defer explicit_id.deinit(alloc);
    const explicit_runtime = try deriveRuntimeTableSchema(alloc, explicit_id);
    defer storage_schema.freeSchema(alloc, explicit_runtime);
    try std.testing.expectEqual(@as(usize, 2), explicit_runtime.index_sort.len);
    try std.testing.expectEqualStrings("_id", explicit_runtime.index_sort[1].field);

    var match_mapping_type = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"dates","path_match":"meta.*_at","match_mapping_type":"date","mapping":{"type":"datetime","sortable":true}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"meta.created_at","order":"desc"}
        \\  ]
        \\}
    );
    defer match_mapping_type.deinit(alloc);
    const match_mapping_runtime = try deriveRuntimeTableSchema(alloc, match_mapping_type);
    defer storage_schema.freeSchema(alloc, match_mapping_runtime);
    try std.testing.expectEqual(@as(usize, 2), match_mapping_runtime.index_sort.len);
    try std.testing.expectEqualStrings("meta.created_at", match_mapping_runtime.index_sort[0].field);
    try std.testing.expect(match_mapping_runtime.index_sort[0].desc);
    try std.testing.expectEqualStrings("_id", match_mapping_runtime.index_sort[1].field);

    var unsortable = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"rank","path_match":"rank","mapping":{"type":"numeric","sortable":false}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"rank","order":"asc"}
        \\  ]
        \\}
    );
    defer unsortable.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, unsortable));

    var id_not_final = try parseValidatedTableSchema(alloc,
        \\{
        \\  "dynamic_templates": [
        \\    {"name":"created","path_match":"created_at","mapping":{"type":"datetime","sortable":true}}
        \\  ],
        \\  "index_sort": [
        \\    {"field":"_id","order":"asc"},
        \\    {"field":"created_at","order":"asc"}
        \\  ]
        \\}
    );
    defer id_not_final.deinit(alloc);
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, deriveRuntimeTableSchema(alloc, id_not_final));
}
