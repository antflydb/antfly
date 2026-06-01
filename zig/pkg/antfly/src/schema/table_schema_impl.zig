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
const schema_regex = @import("antfly_regex");

pub const StorageMode = enum {
    document,
    relational,

    pub fn fromString(text: []const u8) ?StorageMode {
        if (std.mem.eql(u8, text, "document")) return .document;
        if (std.mem.eql(u8, text, "relational")) return .relational;
        return null;
    }
};

pub const TableSchema = struct {
    version: u32 = 0,
    storage_mode: StorageMode = .document,
    default_type: []const u8 = "",
    ttl_duration_ns: u64 = 0,
    ttl_field: []const u8 = "_timestamp",
    enforce_types: bool = false,
    document_schemas: []DocumentSchema = &.{},
    dynamic_templates: []DynamicTemplate = &.{},

    pub fn deinit(self: *TableSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.default_type);
        alloc.free(self.ttl_field);
        for (self.document_schemas) |*document_schema| document_schema.deinit(alloc);
        if (self.document_schemas.len > 0) alloc.free(self.document_schemas);
        for (self.dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (self.dynamic_templates.len > 0) alloc.free(self.dynamic_templates);
        self.* = undefined;
    }
};

pub const DocumentSchema = struct {
    name: []const u8,
    min_properties: ?u64 = null,
    max_properties: ?u64 = null,
    required_fields: [][]const u8 = &.{},
    include_in_all_fields: [][]const u8 = &.{},
    properties: []DocumentProperty = &.{},
    pattern_properties: []const PatternProperty = &.{},
    additional_properties_allowed: ?bool = null,
    additional_properties_schema: ?*DocumentProperty = null,
    dynamic_infer_types: bool = false,
    unevaluated_properties_allowed: ?bool = null,
    unevaluated_properties_schema: ?*DocumentProperty = null,
    property_names: ?*DocumentProperty = null,
    dependent_required: []const DependentRequired = &.{},
    dependent_schemas: []const DependentSchema = &.{},
    any_of: []DocumentProperty = &.{},
    one_of: []DocumentProperty = &.{},
    all_of: []DocumentProperty = &.{},
    not_schema: ?*DocumentProperty = null,
    if_schema: ?*DocumentProperty = null,
    then_schema: ?*DocumentProperty = null,
    else_schema: ?*DocumentProperty = null,

    pub fn deinit(self: *DocumentSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        for (self.include_in_all_fields) |field_name| alloc.free(field_name);
        if (self.include_in_all_fields.len > 0) alloc.free(self.include_in_all_fields);
        for (self.properties) |*property| property.deinit(alloc);
        if (self.properties.len > 0) alloc.free(self.properties);
        for (self.pattern_properties) |property| {
            var owned = property;
            owned.deinit(alloc);
        }
        if (self.pattern_properties.len > 0) alloc.free(self.pattern_properties);
        if (self.additional_properties_schema) |additional_properties_schema| {
            additional_properties_schema.deinit(alloc);
            alloc.destroy(additional_properties_schema);
        }
        if (self.unevaluated_properties_schema) |unevaluated_properties_schema| {
            unevaluated_properties_schema.deinit(alloc);
            alloc.destroy(unevaluated_properties_schema);
        }
        if (self.property_names) |property_names| {
            property_names.deinit(alloc);
            alloc.destroy(property_names);
        }
        for (self.dependent_required) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_required.len > 0) alloc.free(self.dependent_required);
        for (self.dependent_schemas) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_schemas.len > 0) alloc.free(self.dependent_schemas);
        for (self.any_of) |*property| property.deinit(alloc);
        if (self.any_of.len > 0) alloc.free(self.any_of);
        for (self.one_of) |*property| property.deinit(alloc);
        if (self.one_of.len > 0) alloc.free(self.one_of);
        for (self.all_of) |*property| property.deinit(alloc);
        if (self.all_of.len > 0) alloc.free(self.all_of);
        if (self.not_schema) |not_schema| {
            not_schema.deinit(alloc);
            alloc.destroy(not_schema);
        }
        if (self.if_schema) |if_schema| {
            if_schema.deinit(alloc);
            alloc.destroy(if_schema);
        }
        if (self.then_schema) |then_schema| {
            then_schema.deinit(alloc);
            alloc.destroy(then_schema);
        }
        if (self.else_schema) |else_schema| {
            else_schema.deinit(alloc);
            alloc.destroy(else_schema);
        }
        self.* = undefined;
    }
};

pub const DocumentProperty = struct {
    name: []const u8,
    root_ref: bool = false,
    field_type: ?[]const u8 = null,
    antfly_types: [][]const u8 = &.{},
    analyzer: ?[]const u8 = null,
    antfly_index: ?bool = null,
    integer_only: bool = false,
    format: ?[]const u8 = null,
    allows_null: bool = false,
    const_value: ?[]const u8 = null,
    minimum: ?f64 = null,
    maximum: ?f64 = null,
    exclusive_minimum: ?f64 = null,
    exclusive_maximum: ?f64 = null,
    multiple_of: ?f64 = null,
    min_length: ?u64 = null,
    max_length: ?u64 = null,
    min_properties: ?u64 = null,
    max_properties: ?u64 = null,
    pattern: ?[]const u8 = null,
    min_items: ?u64 = null,
    max_items: ?u64 = null,
    additional_items_allowed: ?bool = null,
    min_contains: ?u64 = null,
    max_contains: ?u64 = null,
    unique_items: bool = false,
    enum_values: [][]const u8 = &.{},
    required_fields: [][]const u8 = &.{},
    include_in_all_fields: [][]const u8 = &.{},
    prefix_items: []DocumentProperty = &.{},
    properties: []DocumentProperty = &.{},
    pattern_properties: []const PatternProperty = &.{},
    additional_properties_allowed: ?bool = null,
    additional_properties_schema: ?*DocumentProperty = null,
    dynamic_infer_types: bool = false,
    unevaluated_properties_allowed: ?bool = null,
    unevaluated_properties_schema: ?*DocumentProperty = null,
    property_names: ?*DocumentProperty = null,
    dependent_required: []const DependentRequired = &.{},
    dependent_schemas: []const DependentSchema = &.{},
    any_of: []DocumentProperty = &.{},
    one_of: []DocumentProperty = &.{},
    all_of: []DocumentProperty = &.{},
    not_schema: ?*DocumentProperty = null,
    if_schema: ?*DocumentProperty = null,
    then_schema: ?*DocumentProperty = null,
    else_schema: ?*DocumentProperty = null,
    contains_schema: ?*DocumentProperty = null,
    item: ?*DocumentProperty = null,
    unevaluated_items_allowed: ?bool = null,
    unevaluated_items_schema: ?*DocumentProperty = null,
    embedded_schema: ?*DocumentProperty = null,
    embedded_dynamic_templates: []DynamicTemplate = &.{},

    pub fn deinit(self: *DocumentProperty, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.field_type) |field_type| alloc.free(field_type);
        for (self.antfly_types) |antfly_type| alloc.free(antfly_type);
        if (self.antfly_types.len > 0) alloc.free(self.antfly_types);
        if (self.analyzer) |analyzer| alloc.free(analyzer);
        if (self.format) |format| alloc.free(format);
        if (self.const_value) |const_value| alloc.free(const_value);
        if (self.pattern) |pattern| alloc.free(pattern);
        for (self.enum_values) |enum_value| alloc.free(enum_value);
        if (self.enum_values.len > 0) alloc.free(self.enum_values);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        for (self.include_in_all_fields) |field_name| alloc.free(field_name);
        if (self.include_in_all_fields.len > 0) alloc.free(self.include_in_all_fields);
        for (self.prefix_items) |*property| property.deinit(alloc);
        if (self.prefix_items.len > 0) alloc.free(self.prefix_items);
        for (self.properties) |*property| property.deinit(alloc);
        if (self.properties.len > 0) alloc.free(self.properties);
        for (self.pattern_properties) |property| {
            var owned = property;
            owned.deinit(alloc);
        }
        if (self.pattern_properties.len > 0) alloc.free(self.pattern_properties);
        if (self.additional_properties_schema) |additional_properties_schema| {
            additional_properties_schema.deinit(alloc);
            alloc.destroy(additional_properties_schema);
        }
        if (self.unevaluated_properties_schema) |unevaluated_properties_schema| {
            unevaluated_properties_schema.deinit(alloc);
            alloc.destroy(unevaluated_properties_schema);
        }
        if (self.property_names) |property_names| {
            property_names.deinit(alloc);
            alloc.destroy(property_names);
        }
        for (self.dependent_required) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_required.len > 0) alloc.free(self.dependent_required);
        for (self.dependent_schemas) |dependency| {
            var owned = dependency;
            owned.deinit(alloc);
        }
        if (self.dependent_schemas.len > 0) alloc.free(self.dependent_schemas);
        for (self.any_of) |*property| property.deinit(alloc);
        if (self.any_of.len > 0) alloc.free(self.any_of);
        for (self.one_of) |*property| property.deinit(alloc);
        if (self.one_of.len > 0) alloc.free(self.one_of);
        for (self.all_of) |*property| property.deinit(alloc);
        if (self.all_of.len > 0) alloc.free(self.all_of);
        if (self.not_schema) |not_schema| {
            not_schema.deinit(alloc);
            alloc.destroy(not_schema);
        }
        if (self.if_schema) |if_schema| {
            if_schema.deinit(alloc);
            alloc.destroy(if_schema);
        }
        if (self.then_schema) |then_schema| {
            then_schema.deinit(alloc);
            alloc.destroy(then_schema);
        }
        if (self.else_schema) |else_schema| {
            else_schema.deinit(alloc);
            alloc.destroy(else_schema);
        }
        if (self.contains_schema) |contains_schema| {
            contains_schema.deinit(alloc);
            alloc.destroy(contains_schema);
        }
        if (self.item) |item| {
            item.deinit(alloc);
            alloc.destroy(item);
        }
        if (self.unevaluated_items_schema) |unevaluated_items_schema| {
            unevaluated_items_schema.deinit(alloc);
            alloc.destroy(unevaluated_items_schema);
        }
        if (self.embedded_schema) |embedded_schema| {
            embedded_schema.deinit(alloc);
            alloc.destroy(embedded_schema);
        }
        for (self.embedded_dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (self.embedded_dynamic_templates.len > 0) alloc.free(self.embedded_dynamic_templates);
        self.* = undefined;
    }
};

pub const DependentRequired = struct {
    name: []const u8,
    required_fields: [][]const u8 = &.{},

    pub fn deinit(self: *DependentRequired, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.required_fields) |field_name| alloc.free(field_name);
        if (self.required_fields.len > 0) alloc.free(self.required_fields);
        self.* = undefined;
    }
};

pub const DependentSchema = struct {
    name: []const u8,
    schema: *DocumentProperty,

    pub fn deinit(self: *DependentSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        self.schema.deinit(alloc);
        alloc.destroy(self.schema);
        self.* = undefined;
    }
};

pub const PatternProperty = struct {
    pattern: []const u8,
    property: *DocumentProperty,

    pub fn deinit(self: *PatternProperty, alloc: std.mem.Allocator) void {
        alloc.free(self.pattern);
        self.property.deinit(alloc);
        alloc.destroy(self.property);
        self.* = undefined;
    }
};

pub const DynamicTemplate = struct {
    name: []const u8,
    match_pattern: ?[]const u8 = null,
    unmatch_pattern: ?[]const u8 = null,
    path_match: ?[]const u8 = null,
    path_unmatch: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    field_type: ?[]const u8 = null,
    analyzer: ?[]const u8 = null,
    do_index: ?bool = null,
    store: ?bool = null,
    doc_values: ?bool = null,
    include_in_all: ?bool = null,

    pub fn deinit(self: *DynamicTemplate, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.match_pattern) |match_pattern| alloc.free(match_pattern);
        if (self.unmatch_pattern) |unmatch_pattern| alloc.free(unmatch_pattern);
        if (self.path_match) |path_match| alloc.free(path_match);
        if (self.path_unmatch) |path_unmatch| alloc.free(path_unmatch);
        if (self.match_mapping_type) |match_mapping_type| alloc.free(match_mapping_type);
        if (self.field_type) |field_type| alloc.free(field_type);
        if (self.analyzer) |analyzer| alloc.free(analyzer);
        self.* = undefined;
    }
};

const ParsedTypeSpec = struct {
    field_type: ?[]const u8 = null,
    integer_only: bool = false,
    allows_null: bool = false,
};

const SchemaContext = struct {
    document_root: std.json.ObjectMap,
    scope_schema: std.json.ObjectMap,

    fn child(self: SchemaContext, object: std.json.ObjectMap) SchemaContext {
        return .{
            .document_root = self.document_root,
            .scope_schema = if (object.get("$defs") != null) object else self.scope_schema,
        };
    }
};

const RuntimeValidationContext = struct {
    alloc: std.mem.Allocator,
    root_property: ?*const DocumentProperty = null,
    active_root_ref_values: std.ArrayListUnmanaged(usize) = .{ .items = &.{}, .capacity = 0 },

    fn deinit(self: *RuntimeValidationContext) void {
        self.active_root_ref_values.deinit(self.alloc);
        self.* = undefined;
    }

    fn rootRefGuard(self: *RuntimeValidationContext, value: *const std.json.Value) !?RootRefGuard {
        const root_property = self.root_property orelse return error.InvalidBatchRequest;
        _ = root_property;
        const value_addr = @intFromPtr(value);
        for (self.active_root_ref_values.items) |active| {
            if (active == value_addr) return null;
        }
        try self.active_root_ref_values.append(self.alloc, value_addr);
        return .{ .ctx = self };
    }
};

const RootRefGuard = struct {
    ctx: *RuntimeValidationContext,

    fn release(self: RootRefGuard) void {
        _ = self.ctx.active_root_ref_values.pop();
    }
};

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidSchemaUpdateRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    try validateSchemaValue(parsed.value);
    var schema = try parseTableSchemaValue(alloc, parsed.value);
    defer schema.deinit(alloc);
    try validateParsedTtlSchema(schema);
    try validateParsedRelationalSchema(schema);
    return try stringifyJsonValue(alloc, parsed.value);
}

pub fn parseSchema(alloc: std.mem.Allocator, schema_json: []const u8) !TableSchema {
    if (schema_json.len == 0) {
        return .{
            .default_type = try alloc.dupe(u8, ""),
            .ttl_field = try alloc.dupe(u8, "_timestamp"),
        };
    }

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();
    try validateSchemaValue(parsed.value);
    const schema = try parseTableSchemaValue(alloc, parsed.value);
    errdefer {
        var owned = schema;
        owned.deinit(alloc);
    }
    try validateParsedTtlSchema(schema);
    try validateParsedRelationalSchema(schema);
    return schema;
}

pub fn validateJsonSchemaJson(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    value_json: []const u8,
) !void {
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer schema_parsed.deinit();
    var value_parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer value_parsed.deinit();
    try validateJsonSchemaValue(alloc, schema_parsed.value, value_parsed.value);
}

pub fn validateJsonSchemaValue(
    alloc: std.mem.Allocator,
    schema: std.json.Value,
    value: std.json.Value,
) !void {
    const schema_object = switch (schema) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    const context: SchemaContext = .{
        .document_root = schema_object,
        .scope_schema = schema_object,
    };
    const root_property = try parseAnonymousProperty(alloc, context, schema_object);
    defer {
        root_property.deinit(alloc);
        alloc.destroy(root_property);
    }

    var validation_context = RuntimeValidationContext{
        .alloc = alloc,
        .root_property = root_property,
    };
    defer validation_context.deinit();

    try validateDocumentFieldValueWithContext(&validation_context, root_property.*, &value, false);
}

pub fn documentTtlTimestampNs(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    value_json: []const u8,
) !?u64 {
    if (schema.ttl_duration_ns == 0) return null;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidBatchRequest,
    };

    const ttl_value = root.get(schema.ttl_field) orelse return null;
    if (ttl_value == .null) return null;
    return try parseTtlTimestampNs(ttl_value);
}

pub fn validateWritesAgainstSchema(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    writes: anytype,
) !void {
    const Writes = @TypeOf(writes);
    switch (@typeInfo(Writes)) {
        .pointer => |pointer| {
            if (pointer.size == .slice) {
                for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
                return;
            }
            if (pointer.size == .one) {
                const child = @typeInfo(pointer.child);
                if (child == .array) {
                    for (writes.*) |write| try validateDocumentJson(alloc, schema, write.value);
                    return;
                }
                if (child == .@"struct" and child.@"struct".is_tuple) {
                    inline for (writes.*) |write| try validateDocumentJson(alloc, schema, write.value);
                    return;
                }
            }
        },
        .array => {
            for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
            return;
        },
        .@"struct" => |struct_info| {
            if (struct_info.is_tuple) {
                inline for (writes) |write| try validateDocumentJson(alloc, schema, write.value);
                return;
            }
        },
        else => {},
    }
    @compileError("validateWritesAgainstSchema expects a slice, array, or tuple of writes");
}

pub fn validateDocumentJson(
    alloc: std.mem.Allocator,
    schema: TableSchema,
    value_json: []const u8,
) !void {
    if (schema.document_schemas.len == 0 and !schema.enforce_types and schema.ttl_duration_ns == 0 and schema.dynamic_templates.len == 0) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidBatchRequest,
    };

    const document_schema = try resolveDocumentSchema(schema, root);
    var validation_context = RuntimeValidationContext{ .alloc = alloc };
    defer validation_context.deinit();
    var root_property: ?DocumentProperty = null;
    var root_composition_evaluated_fields = std.StringHashMapUnmanaged(void).empty;
    defer root_composition_evaluated_fields.deinit(alloc);
    if (document_schema) |resolved_document_schema| {
        root_property = makeRootDocumentProperty(resolved_document_schema);
        validation_context.root_property = &root_property.?;
        try validateDocumentFieldValueWithContext(&validation_context, root_property.?, &parsed.value, false);
        try collectComposedObjectFieldCoverage(&validation_context, root_property.?, root, schema.enforce_types, &root_composition_evaluated_fields, false);
    }
    var it = root.iterator();
    while (it.next()) |entry| {
        const field_name = entry.key_ptr.*;
        if (schema.ttl_duration_ns > 0 and std.mem.eql(u8, field_name, schema.ttl_field)) {
            try validateTtlFieldValue(entry.value_ptr.*);
            continue;
        }
        if (shouldIgnoreSchemaValidationField(field_name)) continue;

        if (document_schema) |resolved_document_schema| {
            if (findDocumentProperty(resolved_document_schema.properties, field_name)) |property| {
                try validateDocumentFieldValueWithContext(&validation_context, property, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (try validatePatternProperties(&validation_context, field_name, entry.value_ptr, resolved_document_schema.pattern_properties, schema.enforce_types)) {
                continue;
            }
        }
        if (fieldMatchesDynamicTemplates(schema.dynamic_templates, field_name, entry.value_ptr.*)) continue;
        if (document_schema) |resolved_document_schema| {
            if (resolved_document_schema.additional_properties_schema) |additional_properties_schema| {
                try validateDocumentFieldValueWithContext(&validation_context, additional_properties_schema.*, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (resolved_document_schema.additional_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
        }
        if (root_composition_evaluated_fields.contains(field_name)) continue;
        if (root_property) |resolved_root_property| {
            if (resolved_root_property.unevaluated_properties_schema) |unevaluated_properties_schema| {
                try validateDocumentFieldValueWithContext(&validation_context, unevaluated_properties_schema.*, entry.value_ptr, schema.enforce_types);
                continue;
            }
            if (resolved_root_property.unevaluated_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
        }
        if (schema.enforce_types) return error.InvalidBatchRequest;
    }
}

pub fn globMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == text[ti] or pattern[pi] == '?')) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn validateSchemaValue(value: std.json.Value) !void {
    const root = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    if (root.get("version")) |version| if (version != .null) try validateNonNegativeInteger(version);
    if (root.get("storage_mode")) |storage_mode| if (storage_mode != .null) switch (storage_mode) {
        .string => |text| {
            if (StorageMode.fromString(text) == null) return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (root.get("default_type")) |default_type| if (default_type != .null and default_type != .string) return error.InvalidSchemaUpdateRequest;
    if (root.get("ttl_duration_ns")) |ttl_duration_ns| if (ttl_duration_ns != .null) try validateNonNegativeInteger(ttl_duration_ns);
    if (root.get("ttl_field")) |ttl_field| if (ttl_field != .null) switch (ttl_field) {
        .string => |text| {
            if (text.len == 0) return error.InvalidSchemaUpdateRequest;
        },
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (root.get("enforce_types")) |enforce_types| if (enforce_types != .null and enforce_types != .bool) return error.InvalidSchemaUpdateRequest;
    if (root.get("document_schemas")) |document_schemas| if (document_schemas != .null) try validateDocumentSchemas(document_schemas);
    if (root.get("dynamic_templates")) |dynamic_templates| if (dynamic_templates != .null) try validateDynamicTemplates(dynamic_templates);
}

fn validateDocumentSchemas(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        const doc_schema = switch (entry.value_ptr.*) {
            .object => |doc_schema| doc_schema,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const schema_value = doc_schema.get("schema") orelse return error.InvalidSchemaUpdateRequest;
        try validateDocumentSchemaDefinition(schema_value);
    }
}

fn validateDocumentSchemaDefinition(value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    return validateDocumentSchemaDefinitionWithContext(.{
        .document_root = object,
        .scope_schema = object,
    }, value);
}

fn validateDocumentSchemaDefinitionWithContext(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const current_context = context.child(object);
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (!isRootSchemaRef(ref_path)) {
            try validateDocumentSchemaDefinitionWithContext(current_context, .{
                .object = try resolveSchemaRef(current_context, ref_path),
            });
        }
    }
    try validateDocumentSchemaKeywords(current_context, object);
}

fn validateDocumentSchemaKeywords(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    if (object.get("type")) |schema_type| {
        if (schema_type != .null) _ = try validateTypeSpecDefinition(schema_type, true);
    }
    if (object.get("format")) |format| {
        if (format != .null and format != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("nullable")) |nullable| {
        if (nullable != .null and nullable != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-include-in-all")) |include_in_all| {
        if (include_in_all != .null) try validateAntflyIncludeInAllDefinition(include_in_all);
    }
    if (object.get("schema")) |embedded_schema| {
        if (embedded_schema != .null) try validateDocumentSchemaDefinition(embedded_schema);
    }
    if (object.get("dynamic_templates")) |dynamic_templates| {
        if (dynamic_templates != .null) try validateDynamicTemplates(dynamic_templates);
    }
    if (object.get("$defs")) |definitions| {
        if (definitions != .null) try validateDefinitionsDefinition(context, definitions);
    }
    if (object.get("properties")) |properties| {
        if (properties != .null) {
            if (properties != .object) return error.InvalidSchemaUpdateRequest;
            var it = properties.object.iterator();
            while (it.next()) |entry| try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
        }
    }
    if (object.get("required")) |required| {
        if (required != .null) {
            if (required != .array) return error.InvalidSchemaUpdateRequest;
            for (required.array.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        }
    }
    if (object.get("propertyNames")) |property_names| {
        if (property_names != .null) try validatePropertySchemaDefinitionWithContext(context, property_names);
    }
    if (object.get("patternProperties")) |pattern_properties| {
        if (pattern_properties != .null) try validatePatternPropertiesDefinition(context, pattern_properties);
    }
    if (object.get("additionalProperties")) |additional_properties| {
        if (additional_properties != .null and additional_properties != .bool and additional_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (additional_properties == .object) try validatePropertySchemaDefinitionWithContext(context, additional_properties);
    }
    if (object.get("unevaluatedProperties")) |unevaluated_properties| {
        if (unevaluated_properties != .null and unevaluated_properties != .bool and unevaluated_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_properties == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_properties);
    }
    if (object.get("dependentRequired")) |dependent_required| {
        if (dependent_required != .null) try validateDependentRequiredDefinition(dependent_required);
    }
    if (object.get("dependentSchemas")) |dependent_schemas| {
        if (dependent_schemas != .null) try validateDependentSchemasDefinition(context, dependent_schemas);
    }
    if (object.get("dependencies")) |dependencies| {
        if (dependencies != .null) try validateDependenciesDefinition(context, dependencies);
    }
    if (object.get("items")) |items| {
        if (items != .null) try validatePropertySchemaDefinitionWithContext(context, items);
    }
    if (object.get("prefixItems")) |prefix_items| {
        if (prefix_items != .null) try validatePrefixItemsDefinition(context, prefix_items);
    }
    try validateAdditionalItemsDefinition(context, object);
    if (object.get("unevaluatedItems")) |unevaluated_items| {
        if (unevaluated_items != .null and unevaluated_items != .bool and unevaluated_items != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_items == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_items);
    }
    if (object.get("contains")) |contains| {
        if (contains != .null) try validatePropertySchemaDefinitionWithContext(context, contains);
    }
    if (object.get("const")) |_| {}
    if (object.get("enum")) |enum_values| {
        if (enum_values != .null) try validateEnumDefinition(enum_values);
    }
    if (object.get("minimum")) |minimum| {
        if (minimum != .null) _ = parseJsonNumber(minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("maximum")) |maximum| {
        if (maximum != .null) _ = parseJsonNumber(maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMinimum")) |exclusive_minimum| {
        if (exclusive_minimum != .null) _ = parseJsonNumber(exclusive_minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMaximum")) |exclusive_maximum| {
        if (exclusive_maximum != .null) _ = parseJsonNumber(exclusive_maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("multipleOf")) |multiple_of| {
        if (multiple_of != .null) try validatePositiveNumber(multiple_of);
    }
    if (object.get("anyOf")) |any_of| {
        if (any_of != .null) try validateAnyOfDefinition(context, any_of);
    }
    if (object.get("oneOf")) |one_of| {
        if (one_of != .null) try validateOneOfDefinition(context, one_of);
    }
    if (object.get("allOf")) |all_of| {
        if (all_of != .null) try validateAllOfDefinition(context, all_of);
    }
    if (object.get("not")) |not_schema| {
        if (not_schema != .null) try validatePropertySchemaDefinitionWithContext(context, not_schema);
    }
    if (object.get("if")) |if_schema| {
        if (if_schema != .null) try validatePropertySchemaDefinitionWithContext(context, if_schema);
    }
    if (object.get("then")) |then_schema| {
        if (then_schema != .null) try validatePropertySchemaDefinitionWithContext(context, then_schema);
    }
    if (object.get("else")) |else_schema| {
        if (else_schema != .null) try validatePropertySchemaDefinitionWithContext(context, else_schema);
    }
    if ((object.get("then") != null or object.get("else") != null) and object.get("if") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("pattern")) |pattern| {
        if (pattern != .null and pattern != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("minLength")) |min_length| {
        if (min_length != .null) try validateNonNegativeInteger(min_length);
    }
    if (object.get("maxLength")) |max_length| {
        if (max_length != .null) try validateNonNegativeInteger(max_length);
    }
    if (object.get("minProperties")) |min_properties| {
        if (min_properties != .null) try validateNonNegativeInteger(min_properties);
    }
    if (object.get("maxProperties")) |max_properties| {
        if (max_properties != .null) try validateNonNegativeInteger(max_properties);
    }
    if (object.get("minItems")) |min_items| {
        if (min_items != .null) try validateNonNegativeInteger(min_items);
    }
    if (object.get("maxItems")) |max_items| {
        if (max_items != .null) try validateNonNegativeInteger(max_items);
    }
    if (object.get("minContains")) |min_contains| {
        if (min_contains != .null) try validateNonNegativeInteger(min_contains);
    }
    if (object.get("maxContains")) |max_contains| {
        if (max_contains != .null) try validateNonNegativeInteger(max_contains);
    }
    if ((object.get("minContains") != null or object.get("maxContains") != null) and object.get("contains") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn validatePropertySchemaDefinition(value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    return validatePropertySchemaDefinitionWithContext(.{
        .document_root = object,
        .scope_schema = object,
    }, value);
}

fn validatePropertySchemaDefinitionWithContext(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const current_context = context.child(object);
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (!isRootSchemaRef(ref_path)) {
            try validatePropertySchemaDefinitionWithContext(current_context, .{
                .object = try resolveSchemaRef(current_context, ref_path),
            });
        }
    }
    try validatePropertySchemaKeywords(current_context, object);
}

fn validatePropertySchemaKeywords(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    if (object.get("type")) |schema_type| {
        if (schema_type != .null) _ = try validateTypeSpecDefinition(schema_type, false);
    }
    if (object.get("format")) |format| {
        if (format != .null and format != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("nullable")) |nullable| {
        if (nullable != .null and nullable != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-types")) |antfly_types| {
        if (antfly_types != .null) try validateAntflyTypesDefinition(antfly_types);
    }
    if (object.get("x-antfly-analyzer")) |analyzer| {
        if (analyzer != .null and analyzer != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-index")) |antfly_index| {
        if (antfly_index != .null and antfly_index != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("x-antfly-include-in-all")) |include_in_all| {
        if (include_in_all != .null) try validateAntflyIncludeInAllDefinition(include_in_all);
    }
    if (object.get("$defs")) |definitions| {
        if (definitions != .null) try validateDefinitionsDefinition(context, definitions);
    }
    if (object.get("properties")) |properties| {
        if (properties != .null) {
            if (properties != .object) return error.InvalidSchemaUpdateRequest;
            var it = properties.object.iterator();
            while (it.next()) |entry| try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
        }
    }
    if (object.get("required")) |required| {
        if (required != .null) {
            if (required != .array) return error.InvalidSchemaUpdateRequest;
            for (required.array.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        }
    }
    if (object.get("propertyNames")) |property_names| {
        if (property_names != .null) try validatePropertySchemaDefinitionWithContext(context, property_names);
    }
    if (object.get("patternProperties")) |pattern_properties| {
        if (pattern_properties != .null) try validatePatternPropertiesDefinition(context, pattern_properties);
    }
    if (object.get("additionalProperties")) |additional_properties| {
        if (additional_properties != .null and additional_properties != .bool and additional_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (additional_properties == .object) try validatePropertySchemaDefinitionWithContext(context, additional_properties);
    }
    if (object.get("unevaluatedProperties")) |unevaluated_properties| {
        if (unevaluated_properties != .null and unevaluated_properties != .bool and unevaluated_properties != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_properties == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_properties);
    }
    if (object.get("dependentRequired")) |dependent_required| {
        if (dependent_required != .null) try validateDependentRequiredDefinition(dependent_required);
    }
    if (object.get("dependentSchemas")) |dependent_schemas| {
        if (dependent_schemas != .null) try validateDependentSchemasDefinition(context, dependent_schemas);
    }
    if (object.get("dependencies")) |dependencies| {
        if (dependencies != .null) try validateDependenciesDefinition(context, dependencies);
    }
    if (object.get("items")) |items| {
        if (items != .null) try validatePropertySchemaDefinitionWithContext(context, items);
    }
    if (object.get("prefixItems")) |prefix_items| {
        if (prefix_items != .null) try validatePrefixItemsDefinition(context, prefix_items);
    }
    try validateAdditionalItemsDefinition(context, object);
    if (object.get("unevaluatedItems")) |unevaluated_items| {
        if (unevaluated_items != .null and unevaluated_items != .bool and unevaluated_items != .object) {
            return error.InvalidSchemaUpdateRequest;
        }
        if (unevaluated_items == .object) try validatePropertySchemaDefinitionWithContext(context, unevaluated_items);
    }
    if (object.get("contains")) |contains| {
        if (contains != .null) try validatePropertySchemaDefinitionWithContext(context, contains);
    }
    if (object.get("const")) |_| {}
    if (object.get("enum")) |enum_values| {
        if (enum_values != .null) try validateEnumDefinition(enum_values);
    }
    if (object.get("minimum")) |minimum| {
        if (minimum != .null) _ = parseJsonNumber(minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("maximum")) |maximum| {
        if (maximum != .null) _ = parseJsonNumber(maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMinimum")) |exclusive_minimum| {
        if (exclusive_minimum != .null) _ = parseJsonNumber(exclusive_minimum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("exclusiveMaximum")) |exclusive_maximum| {
        if (exclusive_maximum != .null) _ = parseJsonNumber(exclusive_maximum) catch return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("multipleOf")) |multiple_of| {
        if (multiple_of != .null) try validatePositiveNumber(multiple_of);
    }
    if (object.get("anyOf")) |any_of| {
        if (any_of != .null) try validateAnyOfDefinition(context, any_of);
    }
    if (object.get("oneOf")) |one_of| {
        if (one_of != .null) try validateOneOfDefinition(context, one_of);
    }
    if (object.get("allOf")) |all_of| {
        if (all_of != .null) try validateAllOfDefinition(context, all_of);
    }
    if (object.get("not")) |not_schema| {
        if (not_schema != .null) try validatePropertySchemaDefinitionWithContext(context, not_schema);
    }
    if (object.get("if")) |if_schema| {
        if (if_schema != .null) try validatePropertySchemaDefinitionWithContext(context, if_schema);
    }
    if (object.get("then")) |then_schema| {
        if (then_schema != .null) try validatePropertySchemaDefinitionWithContext(context, then_schema);
    }
    if (object.get("else")) |else_schema| {
        if (else_schema != .null) try validatePropertySchemaDefinitionWithContext(context, else_schema);
    }
    if ((object.get("then") != null or object.get("else") != null) and object.get("if") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("pattern")) |pattern| {
        if (pattern != .null and pattern != .string) return error.InvalidSchemaUpdateRequest;
    }
    if (object.get("minLength")) |min_length| {
        if (min_length != .null) try validateNonNegativeInteger(min_length);
    }
    if (object.get("maxLength")) |max_length| {
        if (max_length != .null) try validateNonNegativeInteger(max_length);
    }
    if (object.get("minProperties")) |min_properties| {
        if (min_properties != .null) try validateNonNegativeInteger(min_properties);
    }
    if (object.get("maxProperties")) |max_properties| {
        if (max_properties != .null) try validateNonNegativeInteger(max_properties);
    }
    if (object.get("minItems")) |min_items| {
        if (min_items != .null) try validateNonNegativeInteger(min_items);
    }
    if (object.get("maxItems")) |max_items| {
        if (max_items != .null) try validateNonNegativeInteger(max_items);
    }
    if (object.get("minContains")) |min_contains| {
        if (min_contains != .null) try validateNonNegativeInteger(min_contains);
    }
    if (object.get("maxContains")) |max_contains| {
        if (max_contains != .null) try validateNonNegativeInteger(max_contains);
    }
    if (object.get("uniqueItems")) |unique_items| {
        if (unique_items != .null and unique_items != .bool) return error.InvalidSchemaUpdateRequest;
    }
    if ((object.get("minContains") != null or object.get("maxContains") != null) and object.get("contains") == null) {
        return error.InvalidSchemaUpdateRequest;
    }
}

fn validateEnumDefinition(value: std.json.Value) !void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
}

fn validateAntflyTypesDefinition(value: std.json.Value) !void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |entry| {
        const type_name = switch (entry) {
            .string => |name| name,
            else => return error.InvalidSchemaUpdateRequest,
        };
        _ = try validateTypeName(type_name, false);
    }
}

fn validateAntflyIncludeInAllDefinition(value: std.json.Value) !void {
    switch (value) {
        .bool => {},
        .array => |arr| {
            for (arr.items) |entry| {
                if (entry != .string) return error.InvalidSchemaUpdateRequest;
            }
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validatePositiveNumber(value: std.json.Value) !void {
    const parsed = parseJsonNumber(value) catch return error.InvalidSchemaUpdateRequest;
    if (parsed <= 0) return error.InvalidSchemaUpdateRequest;
}

fn parseSchemaRefPath(value: std.json.Value) ![]const u8 {
    return switch (value) {
        .string => |ref_path| ref_path,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn isRootSchemaRef(ref_path: []const u8) bool {
    return std.mem.eql(u8, ref_path, "#");
}

fn hasRefSiblings(object: std.json.ObjectMap) bool {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "$ref")) continue;
        if (std.mem.eql(u8, entry.key_ptr.*, "$defs")) continue;
        return true;
    }
    return false;
}

fn resolveSchemaObject(context: SchemaContext, object: std.json.ObjectMap) !std.json.ObjectMap {
    if (object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        return try resolveSchemaRef(context, ref_path);
    }
    return object;
}

fn resolveSchemaRef(context: SchemaContext, ref_path: []const u8) !std.json.ObjectMap {
    if (isRootSchemaRef(ref_path)) return context.document_root;
    if (!std.mem.startsWith(u8, ref_path, "#/")) return error.InvalidSchemaUpdateRequest;

    return resolveSchemaRefWithin(context.scope_schema, ref_path) catch |scope_err| blk: {
        if (resolveSchemaRefWithin(context.document_root, ref_path)) |resolved| break :blk resolved else |root_err| {
            if (scope_err == error.InvalidSchemaUpdateRequest and root_err == error.InvalidSchemaUpdateRequest) {
                return error.InvalidSchemaUpdateRequest;
            }
            return root_err;
        }
    };
}

fn resolveSchemaRefWithin(root_schema: std.json.ObjectMap, ref_path: []const u8) !std.json.ObjectMap {
    var current: std.json.Value = .{ .object = root_schema };
    var tokens = std.mem.tokenizeScalar(u8, ref_path[2..], '/');
    while (tokens.next()) |token| {
        current = switch (current) {
            .object => |object| try resolveSchemaRefToken(object, token),
            else => return error.InvalidSchemaUpdateRequest,
        };
    }

    return switch (current) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
}

fn resolveSchemaRefToken(object: std.json.ObjectMap, token: []const u8) !std.json.Value {
    if (std.mem.indexOfScalar(u8, token, '~') == null) {
        return object.get(token) orelse return error.InvalidSchemaUpdateRequest;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (try jsonPointerTokenMatches(token, entry.key_ptr.*)) return entry.value_ptr.*;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn jsonPointerTokenMatches(encoded_token: []const u8, key: []const u8) !bool {
    var token_index: usize = 0;
    var key_index: usize = 0;
    while (token_index < encoded_token.len and key_index < key.len) {
        const decoded: u8 = blk: {
            switch (encoded_token[token_index]) {
                '~' => {
                    token_index += 1;
                    if (token_index >= encoded_token.len) return error.InvalidSchemaUpdateRequest;
                    break :blk switch (encoded_token[token_index]) {
                        '0' => @as(u8, '~'),
                        '1' => @as(u8, '/'),
                        else => return error.InvalidSchemaUpdateRequest,
                    };
                },
                else => break :blk encoded_token[token_index],
            }
        };
        if (decoded != key[key_index]) return false;
        token_index += 1;
        key_index += 1;
    }
    return token_index == encoded_token.len and key_index == key.len;
}

fn validateTypeSpecDefinition(value: std.json.Value, require_object_only: bool) !ParsedTypeSpec {
    return switch (value) {
        .string => |schema_type_name| blk: {
            const validated_type = try validateTypeName(schema_type_name, require_object_only);
            break :blk .{
                .field_type = validated_type,
                .integer_only = std.mem.eql(u8, validated_type, "integer"),
            };
        },
        .array => |schema_types| try validateTypeArrayDefinition(schema_types, require_object_only),
        else => return error.InvalidSchemaUpdateRequest,
    };
}

fn validateTypeArrayDefinition(value: std.json.Array, require_object_only: bool) !ParsedTypeSpec {
    var parsed = ParsedTypeSpec{};
    if (value.items.len == 0) return error.InvalidSchemaUpdateRequest;

    for (value.items) |entry| {
        const schema_type_name = switch (entry) {
            .string => |schema_type_name| schema_type_name,
            else => return error.InvalidSchemaUpdateRequest,
        };
        if (std.mem.eql(u8, schema_type_name, "null")) {
            if (parsed.allows_null) return error.InvalidSchemaUpdateRequest;
            parsed.allows_null = true;
            continue;
        }
        if (parsed.field_type != null) return error.InvalidSchemaUpdateRequest;
        parsed.field_type = try validateTypeName(schema_type_name, require_object_only);
        parsed.integer_only = std.mem.eql(u8, parsed.field_type.?, "integer");
    }

    if (parsed.field_type == null and !parsed.allows_null) return error.InvalidSchemaUpdateRequest;
    return parsed;
}

fn validateTypeName(schema_type_name: []const u8, require_object_only: bool) ![]const u8 {
    if (require_object_only) {
        if (!std.mem.eql(u8, schema_type_name, "object")) return error.InvalidSchemaUpdateRequest;
        return schema_type_name;
    }
    if (std.mem.eql(u8, schema_type_name, "text") or
        std.mem.eql(u8, schema_type_name, "keyword") or
        std.mem.eql(u8, schema_type_name, "link") or
        std.mem.eql(u8, schema_type_name, "blob") or
        std.mem.eql(u8, schema_type_name, "html") or
        std.mem.eql(u8, schema_type_name, "search_as_you_type") or
        std.mem.eql(u8, schema_type_name, "string") or
        std.mem.eql(u8, schema_type_name, "number") or
        std.mem.eql(u8, schema_type_name, "integer") or
        std.mem.eql(u8, schema_type_name, "null") or
        std.mem.eql(u8, schema_type_name, "numeric") or
        std.mem.eql(u8, schema_type_name, "boolean") or
        std.mem.eql(u8, schema_type_name, "datetime") or
        std.mem.eql(u8, schema_type_name, "geopoint") or
        std.mem.eql(u8, schema_type_name, "json") or
        std.mem.eql(u8, schema_type_name, "object") or
        std.mem.eql(u8, schema_type_name, "array"))
    {
        return schema_type_name;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn validateDependentRequiredDefinition(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        const required = switch (entry.value_ptr.*) {
            .array => |required| required,
            else => return error.InvalidSchemaUpdateRequest,
        };
        for (required.items) |required_value| {
            if (required_value != .string) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn validateDefinitionsDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validateDependentSchemasDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validateDependenciesDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        switch (entry.value_ptr.*) {
            .array => {
                for (entry.value_ptr.array.items) |required_value| {
                    if (required_value != .string) return error.InvalidSchemaUpdateRequest;
                }
            },
            .object => try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*),
            else => return error.InvalidSchemaUpdateRequest,
        }
    }
}

fn validatePatternPropertiesDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        try validatePropertySchemaDefinitionWithContext(context, entry.value_ptr.*);
    }
}

fn validatePrefixItemsDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |item| try validatePropertySchemaDefinitionWithContext(context, item);
}

fn validateAnyOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateOneOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateAllOfDefinition(context: SchemaContext, value: std.json.Value) anyerror!void {
    if (value != .array) return error.InvalidSchemaUpdateRequest;
    for (value.array.items) |variant| try validatePropertySchemaDefinitionWithContext(context, variant);
}

fn validateAdditionalItemsDefinition(context: SchemaContext, object: std.json.ObjectMap) anyerror!void {
    const additional_items = object.get("additionalItems") orelse return;
    if (additional_items == .null) return;
    if (object.get("prefixItems") == null) return error.InvalidSchemaUpdateRequest;
    if (object.get("items") != null) return error.InvalidSchemaUpdateRequest;
    if (additional_items != .bool and additional_items != .object) return error.InvalidSchemaUpdateRequest;
    if (additional_items == .object) try validatePropertySchemaDefinitionWithContext(context, additional_items);
}

fn validateDynamicTemplates(value: std.json.Value) !void {
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| try validateDynamicTemplate(entry.value_ptr.*);
        },
        .array => |array| {
            for (array.items) |entry| try validateDynamicTemplate(entry);
        },
        else => return error.InvalidSchemaUpdateRequest,
    }
}

fn validateDynamicTemplate(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    if (object.get("match")) |match| if (match != .null and match != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("match_pattern")) |match_pattern| if (match_pattern != .null and match_pattern != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("unmatch")) |unmatch| if (unmatch != .null and unmatch != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("path_match")) |path_match| if (path_match != .null and path_match != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("path_unmatch")) |path_unmatch| if (path_unmatch != .null and path_unmatch != .string) return error.InvalidSchemaUpdateRequest;
    if (object.get("match_mapping_type")) |match_mapping_type| {
        if (match_mapping_type != .null and match_mapping_type != .string) return error.InvalidSchemaUpdateRequest;
        if (match_mapping_type == .string and
            !std.mem.eql(u8, match_mapping_type.string, "string") and
            !std.mem.eql(u8, match_mapping_type.string, "number") and
            !std.mem.eql(u8, match_mapping_type.string, "boolean") and
            !std.mem.eql(u8, match_mapping_type.string, "date") and
            !std.mem.eql(u8, match_mapping_type.string, "object"))
        {
            return error.InvalidSchemaUpdateRequest;
        }
    }

    const mapping = object.get("mapping") orelse return error.InvalidSchemaUpdateRequest;
    if (mapping == .null) return error.InvalidSchemaUpdateRequest;
    if (mapping != .object) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("type")) |mapping_type| if (mapping_type != .null and mapping_type != .string) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("analyzer")) |analyzer| if (analyzer != .null and analyzer != .string) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("index")) |index| if (index != .null and index != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("store")) |store| if (store != .null and store != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("doc_values")) |doc_values| if (doc_values != .null and doc_values != .bool) return error.InvalidSchemaUpdateRequest;
    if (mapping.object.get("include_in_all")) |include_in_all| if (include_in_all != .null and include_in_all != .bool) return error.InvalidSchemaUpdateRequest;
}

fn validateNonNegativeInteger(value: std.json.Value) !void {
    const integer = switch (value) {
        .integer => |integer| integer,
        else => return error.InvalidSchemaUpdateRequest,
    };
    if (integer < 0) return error.InvalidSchemaUpdateRequest;
}

fn parseTableSchemaValue(alloc: std.mem.Allocator, value: std.json.Value) !TableSchema {
    const root = value.object;

    var parsed: TableSchema = .{
        .default_type = try alloc.dupe(u8, ""),
        .ttl_field = try alloc.dupe(u8, "_timestamp"),
    };
    errdefer parsed.deinit(alloc);

    if (root.get("version")) |version| {
        if (version != .null) parsed.version = std.math.cast(u32, version.integer) orelse return error.InvalidSchemaUpdateRequest;
    }
    if (root.get("storage_mode")) |storage_mode| {
        if (storage_mode != .null) {
            if (storage_mode != .string) return error.InvalidSchemaUpdateRequest;
            parsed.storage_mode = StorageMode.fromString(storage_mode.string) orelse return error.InvalidSchemaUpdateRequest;
        }
    }
    if (root.get("default_type")) |default_type| {
        if (default_type != .null) {
            alloc.free(parsed.default_type);
            parsed.default_type = try alloc.dupe(u8, default_type.string);
        }
    }
    if (root.get("ttl_duration_ns")) |ttl_duration_ns| {
        if (ttl_duration_ns != .null) parsed.ttl_duration_ns = std.math.cast(u64, ttl_duration_ns.integer) orelse return error.InvalidSchemaUpdateRequest;
    }
    if (root.get("ttl_field")) |ttl_field| {
        if (ttl_field != .null) {
            if (ttl_field.string.len == 0) return error.InvalidSchemaUpdateRequest;
            alloc.free(parsed.ttl_field);
            parsed.ttl_field = try alloc.dupe(u8, ttl_field.string);
        }
    }
    if (root.get("enforce_types")) |enforce_types| {
        if (enforce_types != .null) parsed.enforce_types = enforce_types.bool;
    }
    if (root.get("document_schemas")) |document_schemas| {
        if (document_schemas != .null) parsed.document_schemas = try parseDocumentSchemas(alloc, document_schemas);
    }
    if (root.get("dynamic_templates")) |dynamic_templates| {
        if (dynamic_templates != .null) parsed.dynamic_templates = try parseDynamicTemplates(alloc, dynamic_templates);
    }
    if (parsed.storage_mode == .relational) {
        if (root.get("enforce_types")) |enforce_types| {
            if (enforce_types != .null and !enforce_types.bool) return error.InvalidSchemaUpdateRequest;
        }
        parsed.enforce_types = true;
    }
    return parsed;
}

fn validateParsedTtlSchema(schema: TableSchema) !void {
    if (schema.ttl_duration_ns == 0) return;
    if (schema.ttl_field.len == 0) return error.InvalidSchemaUpdateRequest;

    for (schema.document_schemas) |document_schema| {
        if (findDocumentProperty(document_schema.properties, schema.ttl_field)) |property| {
            const field_type = property.field_type orelse return error.InvalidSchemaUpdateRequest;
            if (!std.mem.eql(u8, field_type, "datetime") and !std.mem.eql(u8, field_type, "numeric")) {
                return error.InvalidSchemaUpdateRequest;
            }
        }
    }
}

fn validateParsedRelationalSchema(schema: TableSchema) !void {
    if (schema.storage_mode != .relational) return;
    if (!schema.enforce_types) return error.InvalidSchemaUpdateRequest;
    if (schema.dynamic_templates.len > 0) return error.InvalidSchemaUpdateRequest;
    if (schema.document_schemas.len != 1) return error.InvalidSchemaUpdateRequest;

    var relational_columns: usize = 0;
    for (schema.document_schemas) |document_schema| {
        if (document_schema.additional_properties_schema != null) return error.InvalidSchemaUpdateRequest;
        if (document_schema.additional_properties_allowed orelse false) return error.InvalidSchemaUpdateRequest;
        if (document_schema.pattern_properties.len > 0) return error.InvalidSchemaUpdateRequest;
        if (document_schema.dynamic_infer_types) return error.InvalidSchemaUpdateRequest;

        for (document_schema.properties) |property| {
            try validateRelationalEmbeddedJsonProperty(property);
            if (isRelationalStorageProperty(property)) relational_columns += 1;
        }
    }

    if (relational_columns == 0) return error.InvalidSchemaUpdateRequest;
}

fn validateRelationalEmbeddedJsonProperty(property: DocumentProperty) !void {
    const has_embedded_document_config = property.embedded_schema != null or property.embedded_dynamic_templates.len > 0;
    if (has_embedded_document_config and !isExplicitJsonProperty(property)) return error.InvalidSchemaUpdateRequest;

    if (property.embedded_schema) |embedded_schema| try validateRelationalEmbeddedJsonProperty(embedded_schema.*);
    if (property.item) |item| try validateRelationalEmbeddedJsonProperty(item.*);
    for (property.properties) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.prefix_items) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.pattern_properties) |child| try validateRelationalEmbeddedJsonProperty(child.property.*);
    if (property.additional_properties_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.unevaluated_properties_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.unevaluated_items_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    for (property.dependent_schemas) |dependent| try validateRelationalEmbeddedJsonProperty(dependent.schema.*);
    for (property.any_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.one_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    for (property.all_of) |child| try validateRelationalEmbeddedJsonProperty(child);
    if (property.not_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.if_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.then_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.else_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
    if (property.contains_schema) |child| try validateRelationalEmbeddedJsonProperty(child.*);
}

fn isExplicitJsonProperty(property: DocumentProperty) bool {
    const field_type = property.field_type orelse return false;
    return std.mem.eql(u8, field_type, "json");
}

fn isRelationalStorageProperty(property: DocumentProperty) bool {
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "embedding")) return false;
        if (std.mem.eql(u8, field_type, "keyword") or
            std.mem.eql(u8, field_type, "link") or
            std.mem.eql(u8, field_type, "string") or
            std.mem.eql(u8, field_type, "text") or
            std.mem.eql(u8, field_type, "html") or
            std.mem.eql(u8, field_type, "search_as_you_type") or
            std.mem.eql(u8, field_type, "boolean") or
            std.mem.eql(u8, field_type, "datetime") or
            std.mem.eql(u8, field_type, "integer") or
            std.mem.eql(u8, field_type, "numeric") or
            std.mem.eql(u8, field_type, "number") or
            std.mem.eql(u8, field_type, "geopoint") or
            std.mem.eql(u8, field_type, "geoshape") or
            std.mem.eql(u8, field_type, "blob") or
            std.mem.eql(u8, field_type, "json") or
            std.mem.eql(u8, field_type, "object") or
            std.mem.eql(u8, field_type, "array")) return true;
        return property.integer_only;
    }
    if (property.integer_only) return true;
    if (property.properties.len > 0 or
        property.item != null or
        (property.additional_properties_allowed orelse false) or
        property.additional_properties_schema != null or
        property.pattern_properties.len > 0 or
        property.dynamic_infer_types) return true;
    return property.const_value != null or property.enum_values.len > 0;
}

fn parseDocumentSchemas(alloc: std.mem.Allocator, value: std.json.Value) ![]DocumentSchema {
    const object = value.object;
    const document_schemas = try alloc.alloc(DocumentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (document_schemas[0..initialized]) |*document_schema| document_schema.deinit(alloc);
        alloc.free(document_schemas);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const schema_value = entry.value_ptr.object.get("schema").?;
        const context: SchemaContext = .{
            .document_root = schema_value.object,
            .scope_schema = schema_value.object,
        };
        const property = try parseAnonymousProperty(alloc, context, schema_value.object);
        defer alloc.destroy(property);
        document_schemas[initialized] = try parseDocumentSchemaFromProperty(alloc, entry.key_ptr.*, property);
        initialized += 1;
    }
    return document_schemas;
}

fn parseDocumentSchemaFromProperty(
    alloc: std.mem.Allocator,
    name: []const u8,
    property: *DocumentProperty,
) !DocumentSchema {
    const document_schema: DocumentSchema = .{
        .name = try alloc.dupe(u8, name),
        .min_properties = property.min_properties,
        .max_properties = property.max_properties,
        .required_fields = property.required_fields,
        .include_in_all_fields = property.include_in_all_fields,
        .properties = property.properties,
        .pattern_properties = property.pattern_properties,
        .additional_properties_allowed = property.additional_properties_allowed,
        .additional_properties_schema = property.additional_properties_schema,
        .dynamic_infer_types = property.dynamic_infer_types,
        .unevaluated_properties_allowed = property.unevaluated_properties_allowed,
        .unevaluated_properties_schema = property.unevaluated_properties_schema,
        .property_names = property.property_names,
        .dependent_required = property.dependent_required,
        .dependent_schemas = property.dependent_schemas,
        .any_of = property.any_of,
        .one_of = property.one_of,
        .all_of = property.all_of,
        .not_schema = property.not_schema,
        .if_schema = property.if_schema,
        .then_schema = property.then_schema,
        .else_schema = property.else_schema,
    };

    property.required_fields = &.{};
    property.include_in_all_fields = &.{};
    property.properties = &.{};
    property.pattern_properties = &.{};
    property.additional_properties_allowed = null;
    property.additional_properties_schema = null;
    property.dynamic_infer_types = false;
    property.unevaluated_properties_allowed = null;
    property.unevaluated_properties_schema = null;
    property.property_names = null;
    property.dependent_required = &.{};
    property.dependent_schemas = &.{};
    property.any_of = &.{};
    property.one_of = &.{};
    property.all_of = &.{};
    property.not_schema = null;
    property.if_schema = null;
    property.then_schema = null;
    property.else_schema = null;
    property.deinit(alloc);
    return document_schema;
}

fn parseDocumentProperties(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) anyerror![]DocumentProperty {
    const properties = try alloc.alloc(DocumentProperty, object.count());
    var initialized: usize = 0;
    errdefer {
        for (properties[0..initialized]) |*property| property.deinit(alloc);
        alloc.free(properties);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const property_object = switch (entry.value_ptr.*) {
            .object => |property_object| property_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const property = try parseAnonymousProperty(alloc, context, property_object);
        defer alloc.destroy(property);
        alloc.free(property.name);
        property.name = try alloc.dupe(u8, entry.key_ptr.*);
        properties[initialized] = property.*;
        initialized += 1;
    }
    return properties;
}

fn parseAnonymousProperty(alloc: std.mem.Allocator, context: SchemaContext, unresolved_object: std.json.ObjectMap) anyerror!*DocumentProperty {
    const current_context = context.child(unresolved_object);
    if (unresolved_object.get("$ref")) |ref_value| {
        const ref_path = try parseSchemaRefPath(ref_value);
        if (isRootSchemaRef(ref_path)) {
            const property = try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
            property.root_ref = true;
            return property;
        }
        const resolved_property = try parseAnonymousProperty(alloc, current_context, try resolveSchemaRef(current_context, ref_path));
        errdefer {
            resolved_property.deinit(alloc);
            alloc.destroy(resolved_property);
        }
        if (!hasRefSiblings(unresolved_object)) return resolved_property;

        const sibling_property = try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
        errdefer {
            sibling_property.deinit(alloc);
            alloc.destroy(sibling_property);
        }

        const combined = try alloc.create(DocumentProperty);
        errdefer alloc.destroy(combined);
        const all_of = try alloc.alloc(DocumentProperty, 2);
        all_of[0] = resolved_property.*;
        all_of[1] = sibling_property.*;
        alloc.destroy(resolved_property);
        alloc.destroy(sibling_property);
        combined.* = .{
            .name = try alloc.dupe(u8, ""),
            .all_of = all_of,
        };
        return combined;
    }
    return try parseAnonymousPropertyKeywords(alloc, current_context, unresolved_object);
}

fn parseAnonymousPropertyKeywords(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) anyerror!*DocumentProperty {
    const property = try alloc.create(DocumentProperty);
    errdefer alloc.destroy(property);

    const type_spec = if (object.get("type")) |property_type|
        try parseTypeSpec(alloc, property_type, false)
    else
        ParsedTypeSpec{};
    const field_type = type_spec.field_type;
    errdefer if (field_type) |owned| alloc.free(owned);
    const format = if (object.get("format")) |format_value|
        switch (format_value) {
            .string => |format_string| try alloc.dupe(u8, format_string),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (format) |owned| alloc.free(owned);
    const allows_null = type_spec.allows_null or parseNullableFlag(object);
    const const_value = if (object.get("const")) |const_schema_value|
        try stringifyJsonValue(alloc, const_schema_value)
    else
        null;
    errdefer if (const_value) |owned| alloc.free(owned);
    const minimum = if (object.get("minimum")) |minimum_value|
        if (minimum_value == .null) null else parseJsonNumber(minimum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const maximum = if (object.get("maximum")) |maximum_value|
        if (maximum_value == .null) null else parseJsonNumber(maximum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const exclusive_minimum = if (object.get("exclusiveMinimum")) |exclusive_minimum_value|
        if (exclusive_minimum_value == .null) null else parseJsonNumber(exclusive_minimum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const exclusive_maximum = if (object.get("exclusiveMaximum")) |exclusive_maximum_value|
        if (exclusive_maximum_value == .null) null else parseJsonNumber(exclusive_maximum_value) catch return error.InvalidSchemaUpdateRequest
    else
        null;
    const multiple_of = if (object.get("multipleOf")) |multiple_of_value|
        if (multiple_of_value == .null) null else blk: {
            const parsed = parseJsonNumber(multiple_of_value) catch return error.InvalidSchemaUpdateRequest;
            if (parsed <= 0) return error.InvalidSchemaUpdateRequest;
            break :blk parsed;
        }
    else
        null;
    const min_length = if (object.get("minLength")) |min_length_value|
        if (min_length_value == .null) null else std.math.cast(u64, min_length_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_length = if (object.get("maxLength")) |max_length_value|
        if (max_length_value == .null) null else std.math.cast(u64, max_length_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const min_properties = if (object.get("minProperties")) |min_properties_value|
        if (min_properties_value == .null) null else std.math.cast(u64, min_properties_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_properties = if (object.get("maxProperties")) |max_properties_value|
        if (max_properties_value == .null) null else std.math.cast(u64, max_properties_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const pattern = if (object.get("pattern")) |pattern_value|
        switch (pattern_value) {
            .string => |pattern_string| try alloc.dupe(u8, pattern_string),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (pattern) |owned| alloc.free(owned);
    const min_items = if (object.get("minItems")) |min_items_value|
        if (min_items_value == .null) null else std.math.cast(u64, min_items_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_items = if (object.get("maxItems")) |max_items_value|
        if (max_items_value == .null) null else std.math.cast(u64, max_items_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const min_contains = if (object.get("minContains")) |min_contains_value|
        if (min_contains_value == .null) null else std.math.cast(u64, min_contains_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const max_contains = if (object.get("maxContains")) |max_contains_value|
        if (max_contains_value == .null) null else std.math.cast(u64, max_contains_value.integer) orelse return error.InvalidSchemaUpdateRequest
    else
        null;
    const unique_items = if (object.get("uniqueItems")) |unique_items_value|
        if (unique_items_value == .null) false else unique_items_value.bool
    else
        false;
    const enum_values: [][]const u8 = if (object.get("enum")) |enum_value|
        if (enum_value == .array) try parseEnumValues(alloc, enum_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (enum_values) |enum_entry| alloc.free(enum_entry);
        if (enum_values.len > 0) alloc.free(enum_values);
    }

    const required_fields: [][]const u8 = if (object.get("required")) |required|
        if (required == .array) try parseRequiredFields(alloc, required.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (required_fields) |field_name| alloc.free(field_name);
        if (required_fields.len > 0) alloc.free(required_fields);
    }
    const antfly_types: [][]const u8 = if (object.get("x-antfly-types")) |types_value|
        if (types_value == .array) try parseRequiredFields(alloc, types_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (antfly_types) |type_name| alloc.free(type_name);
        if (antfly_types.len > 0) alloc.free(antfly_types);
    }
    const analyzer = if (object.get("x-antfly-analyzer")) |analyzer_value|
        switch (analyzer_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    errdefer if (analyzer) |owned| alloc.free(owned);
    const antfly_index = if (object.get("x-antfly-index")) |index_value|
        switch (index_value) {
            .bool => |enabled| enabled,
            .null => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const include_in_all_fields: [][]const u8 = if (object.get("x-antfly-include-in-all")) |include_value|
        if (include_value == .array) try parseRequiredFields(alloc, include_value.array) else &[_][]const u8{}
    else
        &[_][]const u8{};
    errdefer {
        for (include_in_all_fields) |field_name| alloc.free(field_name);
        if (include_in_all_fields.len > 0) alloc.free(include_in_all_fields);
    }
    const embedded_schema = if (object.get("schema")) |schema_value| blk: {
        if (schema_value == .null) break :blk null;
        if (schema_value != .object) return error.InvalidSchemaUpdateRequest;
        const embedded_context: SchemaContext = .{
            .document_root = schema_value.object,
            .scope_schema = schema_value.object,
        };
        break :blk try parseAnonymousProperty(alloc, embedded_context, schema_value.object);
    } else null;
    errdefer if (embedded_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const embedded_dynamic_templates: []DynamicTemplate = if (object.get("dynamic_templates")) |dynamic_templates|
        if (dynamic_templates == .null) &[_]DynamicTemplate{} else try parseDynamicTemplates(alloc, dynamic_templates)
    else
        &[_]DynamicTemplate{};
    errdefer {
        for (embedded_dynamic_templates) |*dynamic_template| dynamic_template.deinit(alloc);
        if (embedded_dynamic_templates.len > 0) alloc.free(embedded_dynamic_templates);
    }
    const prefix_items: []DocumentProperty = if (object.get("prefixItems")) |prefix_items_value|
        if (prefix_items_value == .array) try parsePropertyVariants(alloc, context, prefix_items_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (prefix_items) |prefix_property| {
            var owned = prefix_property;
            owned.deinit(alloc);
        }
        if (prefix_items.len > 0) alloc.free(prefix_items);
    }
    const additional_items_allowed = if (object.get("additionalItems")) |additional_items_value|
        switch (additional_items_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const additional_properties_allowed = if (object.get("additionalProperties")) |additional_properties_value|
        switch (additional_properties_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const additional_properties_schema = if (object.get("additionalProperties")) |additional_properties_value|
        if (additional_properties_value == .object) try parseAnonymousProperty(alloc, context, additional_properties_value.object) else null
    else
        null;
    errdefer if (additional_properties_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const dynamic_infer_types = if (object.get("x-antfly-dynamic-indexing")) |dynamic_indexing_value|
        try parseDynamicIndexingMode(dynamic_indexing_value)
    else
        false;
    const unevaluated_properties_allowed = if (object.get("unevaluatedProperties")) |unevaluated_properties_value|
        switch (unevaluated_properties_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const unevaluated_properties_schema = if (object.get("unevaluatedProperties")) |unevaluated_properties_value|
        if (unevaluated_properties_value == .object) try parseAnonymousProperty(alloc, context, unevaluated_properties_value.object) else null
    else
        null;
    errdefer if (unevaluated_properties_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const pattern_properties = if (object.get("patternProperties")) |pattern_properties_value|
        if (pattern_properties_value == .object) try parsePatternProperties(alloc, context, pattern_properties_value.object) else &[_]PatternProperty{}
    else
        &[_]PatternProperty{};
    errdefer {
        for (pattern_properties) |pattern_property| {
            var owned = pattern_property;
            owned.deinit(alloc);
        }
        if (pattern_properties.len > 0) alloc.free(pattern_properties);
    }

    const child_properties: []DocumentProperty = if (object.get("properties")) |properties_value|
        if (properties_value == .object) try parseDocumentProperties(alloc, context, properties_value.object) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (child_properties) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (child_properties.len > 0) alloc.free(child_properties);
    }
    const property_names = if (object.get("propertyNames")) |property_names_value|
        if (property_names_value == .object) try parseAnonymousProperty(alloc, context, property_names_value.object) else null
    else
        null;
    errdefer if (property_names) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const dependent_required = blk: {
        var explicit = if (object.get("dependentRequired")) |dependent_required_value|
            if (dependent_required_value == .object) try parseDependentRequired(alloc, dependent_required_value.object) else &[_]DependentRequired{}
        else
            &[_]DependentRequired{};
        errdefer freeDependentRequiredSlice(alloc, explicit);
        var legacy = if (object.get("dependencies")) |dependencies_value|
            if (dependencies_value == .object) try parseLegacyDependentRequired(alloc, dependencies_value.object) else &[_]DependentRequired{}
        else
            &[_]DependentRequired{};
        errdefer freeDependentRequiredSlice(alloc, legacy);
        const merged = try mergeDependentRequiredSlices(alloc, explicit, legacy);
        explicit = &[_]DependentRequired{};
        legacy = &[_]DependentRequired{};
        break :blk merged;
    };
    errdefer freeDependentRequiredSlice(alloc, dependent_required);
    const dependent_schemas = blk: {
        var explicit = if (object.get("dependentSchemas")) |dependent_schemas_value|
            if (dependent_schemas_value == .object) try parseDependentSchemas(alloc, context, dependent_schemas_value.object) else &[_]DependentSchema{}
        else
            &[_]DependentSchema{};
        errdefer freeDependentSchemaSlice(alloc, explicit);
        var legacy = if (object.get("dependencies")) |dependencies_value|
            if (dependencies_value == .object) try parseLegacyDependentSchemas(alloc, context, dependencies_value.object) else &[_]DependentSchema{}
        else
            &[_]DependentSchema{};
        errdefer freeDependentSchemaSlice(alloc, legacy);
        const merged = try mergeDependentSchemaSlices(alloc, explicit, legacy);
        explicit = &[_]DependentSchema{};
        legacy = &[_]DependentSchema{};
        break :blk merged;
    };
    errdefer freeDependentSchemaSlice(alloc, dependent_schemas);
    const any_of: []DocumentProperty = if (object.get("anyOf")) |any_of_value|
        if (any_of_value == .array) try parsePropertyVariants(alloc, context, any_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (any_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (any_of.len > 0) alloc.free(any_of);
    }
    const one_of: []DocumentProperty = if (object.get("oneOf")) |one_of_value|
        if (one_of_value == .array) try parsePropertyVariants(alloc, context, one_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (one_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (one_of.len > 0) alloc.free(one_of);
    }
    const all_of: []DocumentProperty = if (object.get("allOf")) |all_of_value|
        if (all_of_value == .array) try parsePropertyVariants(alloc, context, all_of_value.array) else &[_]DocumentProperty{}
    else
        &[_]DocumentProperty{};
    errdefer {
        for (all_of) |child| {
            var owned = child;
            owned.deinit(alloc);
        }
        if (all_of.len > 0) alloc.free(all_of);
    }
    const not_schema = if (object.get("not")) |not_value|
        if (not_value == .object) try parseAnonymousProperty(alloc, context, not_value.object) else null
    else
        null;
    errdefer if (not_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const if_schema = if (object.get("if")) |if_value|
        if (if_value == .object) try parseAnonymousProperty(alloc, context, if_value.object) else null
    else
        null;
    errdefer if (if_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const then_schema = if (object.get("then")) |then_value|
        if (then_value == .object) try parseAnonymousProperty(alloc, context, then_value.object) else null
    else
        null;
    errdefer if (then_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const else_schema = if (object.get("else")) |else_value|
        if (else_value == .object) try parseAnonymousProperty(alloc, context, else_value.object) else null
    else
        null;
    errdefer if (else_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const contains_schema = if (object.get("contains")) |contains_value|
        if (contains_value == .object) try parseAnonymousProperty(alloc, context, contains_value.object) else null
    else
        null;
    errdefer if (contains_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };

    const item = if (object.get("items")) |items_value|
        if (items_value == .object) try parseAnonymousProperty(alloc, context, items_value.object) else null
    else if (object.get("additionalItems")) |additional_items_value|
        if (additional_items_value == .object) try parseAnonymousProperty(alloc, context, additional_items_value.object) else null
    else
        null;
    errdefer if (item) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };
    const unevaluated_items_allowed = if (object.get("unevaluatedItems")) |unevaluated_items_value|
        switch (unevaluated_items_value) {
            .bool => |allowed| allowed,
            .null, .object => null,
            else => return error.InvalidSchemaUpdateRequest,
        }
    else
        null;
    const unevaluated_items_schema = if (object.get("unevaluatedItems")) |unevaluated_items_value|
        if (unevaluated_items_value == .object) try parseAnonymousProperty(alloc, context, unevaluated_items_value.object) else null
    else
        null;
    errdefer if (unevaluated_items_schema) |owned| {
        owned.deinit(alloc);
        alloc.destroy(owned);
    };

    property.* = .{
        .name = try alloc.dupe(u8, ""),
        .root_ref = false,
        .field_type = field_type,
        .antfly_types = antfly_types,
        .analyzer = analyzer,
        .antfly_index = antfly_index,
        .integer_only = type_spec.integer_only,
        .format = format,
        .allows_null = allows_null,
        .const_value = const_value,
        .minimum = minimum,
        .maximum = maximum,
        .exclusive_minimum = exclusive_minimum,
        .exclusive_maximum = exclusive_maximum,
        .multiple_of = multiple_of,
        .min_length = min_length,
        .max_length = max_length,
        .min_properties = min_properties,
        .max_properties = max_properties,
        .pattern = pattern,
        .min_items = min_items,
        .max_items = max_items,
        .additional_items_allowed = additional_items_allowed,
        .min_contains = min_contains,
        .max_contains = max_contains,
        .unique_items = unique_items,
        .enum_values = enum_values,
        .required_fields = required_fields,
        .include_in_all_fields = include_in_all_fields,
        .prefix_items = prefix_items,
        .properties = child_properties,
        .pattern_properties = pattern_properties,
        .additional_properties_allowed = additional_properties_allowed,
        .additional_properties_schema = additional_properties_schema,
        .dynamic_infer_types = dynamic_infer_types,
        .unevaluated_properties_allowed = unevaluated_properties_allowed,
        .unevaluated_properties_schema = unevaluated_properties_schema,
        .property_names = property_names,
        .dependent_required = dependent_required,
        .dependent_schemas = dependent_schemas,
        .any_of = any_of,
        .one_of = one_of,
        .all_of = all_of,
        .not_schema = not_schema,
        .if_schema = if_schema,
        .then_schema = then_schema,
        .else_schema = else_schema,
        .contains_schema = contains_schema,
        .item = item,
        .unevaluated_items_allowed = unevaluated_items_allowed,
        .unevaluated_items_schema = unevaluated_items_schema,
        .embedded_schema = embedded_schema,
        .embedded_dynamic_templates = embedded_dynamic_templates,
    };
    if (property.dynamic_infer_types and (!(property.additional_properties_allowed orelse false) or property.additional_properties_schema != null)) {
        return error.InvalidSchemaUpdateRequest;
    }
    return property;
}

fn parseDynamicIndexingMode(value: std.json.Value) !bool {
    if (value != .object) return error.InvalidSchemaUpdateRequest;
    const mode_value = value.object.get("mode") orelse return error.InvalidSchemaUpdateRequest;
    return switch (mode_value) {
        .string => |mode| if (std.mem.eql(u8, mode, "infer_types")) true else error.InvalidSchemaUpdateRequest,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn parseDependentRequired(alloc: std.mem.Allocator, object: std.json.ObjectMap) ![]DependentRequired {
    const dependencies = try alloc.alloc(DependentRequired, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const required = switch (entry.value_ptr.*) {
            .array => |required| required,
            else => return error.InvalidSchemaUpdateRequest,
        };
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .required_fields = try parseRequiredFields(alloc, required),
        };
        initialized += 1;
    }
    return dependencies;
}

fn parseDependentSchemas(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]DependentSchema {
    const dependencies = try alloc.alloc(DependentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const schema_object = switch (entry.value_ptr.*) {
            .object => |schema_object| schema_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .schema = try parseAnonymousProperty(alloc, context, schema_object),
        };
        initialized += 1;
    }
    return dependencies;
}

fn parseLegacyDependentRequired(alloc: std.mem.Allocator, object: std.json.ObjectMap) ![]DependentRequired {
    const dependencies = try alloc.alloc(DependentRequired, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .array) continue;
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .required_fields = try parseRequiredFields(alloc, entry.value_ptr.array),
        };
        initialized += 1;
    }

    if (initialized == 0) {
        alloc.free(dependencies);
        return &[_]DependentRequired{};
    }
    if (initialized == dependencies.len) return dependencies;

    const trimmed = try alloc.alloc(DependentRequired, initialized);
    @memcpy(trimmed, dependencies[0..initialized]);
    alloc.free(dependencies);
    return trimmed;
}

fn parseLegacyDependentSchemas(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]DependentSchema {
    const dependencies = try alloc.alloc(DependentSchema, object.count());
    var initialized: usize = 0;
    errdefer {
        for (dependencies[0..initialized]) |*dependency| dependency.deinit(alloc);
        alloc.free(dependencies);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        dependencies[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .schema = try parseAnonymousProperty(alloc, context, entry.value_ptr.object),
        };
        initialized += 1;
    }

    if (initialized == 0) {
        alloc.free(dependencies);
        return &[_]DependentSchema{};
    }
    if (initialized == dependencies.len) return dependencies;

    const trimmed = try alloc.alloc(DependentSchema, initialized);
    @memcpy(trimmed, dependencies[0..initialized]);
    alloc.free(dependencies);
    return trimmed;
}

fn freeDependentRequiredSlice(alloc: std.mem.Allocator, dependencies: []const DependentRequired) void {
    for (dependencies) |dependency| {
        var owned = dependency;
        owned.deinit(alloc);
    }
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn freeDependentSchemaSlice(alloc: std.mem.Allocator, dependencies: []const DependentSchema) void {
    for (dependencies) |dependency| {
        var owned = dependency;
        owned.deinit(alloc);
    }
    if (dependencies.len > 0) alloc.free(dependencies);
}

fn mergeDependentRequiredSlices(
    alloc: std.mem.Allocator,
    primary: []const DependentRequired,
    legacy: []const DependentRequired,
) ![]const DependentRequired {
    if (primary.len == 0) return legacy;
    if (legacy.len == 0) return primary;

    const merged = try alloc.alloc(DependentRequired, primary.len + legacy.len);
    @memcpy(merged[0..primary.len], primary);
    @memcpy(merged[primary.len..], legacy);
    alloc.free(primary);
    alloc.free(legacy);
    return merged;
}

fn mergeDependentSchemaSlices(
    alloc: std.mem.Allocator,
    primary: []const DependentSchema,
    legacy: []const DependentSchema,
) ![]const DependentSchema {
    if (primary.len == 0) return legacy;
    if (legacy.len == 0) return primary;

    const merged = try alloc.alloc(DependentSchema, primary.len + legacy.len);
    @memcpy(merged[0..primary.len], primary);
    @memcpy(merged[primary.len..], legacy);
    alloc.free(primary);
    alloc.free(legacy);
    return merged;
}

fn parseTypeSpec(alloc: std.mem.Allocator, value: std.json.Value, require_object_only: bool) !ParsedTypeSpec {
    const validated = try validateTypeSpecDefinition(value, require_object_only);
    return .{
        .field_type = if (validated.field_type) |field_type| try alloc.dupe(u8, field_type) else null,
        .integer_only = validated.integer_only,
        .allows_null = validated.allows_null,
    };
}

fn parseNullableFlag(object: std.json.ObjectMap) bool {
    return if (object.get("nullable")) |nullable|
        switch (nullable) {
            .bool => |enabled| enabled,
            .null => false,
            else => false,
        }
    else
        false;
}

fn parsePatternProperties(alloc: std.mem.Allocator, context: SchemaContext, object: std.json.ObjectMap) ![]PatternProperty {
    const properties = try alloc.alloc(PatternProperty, object.count());
    var initialized: usize = 0;
    errdefer {
        for (properties[0..initialized]) |*property| property.deinit(alloc);
        alloc.free(properties);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const property_object = switch (entry.value_ptr.*) {
            .object => |property_object| property_object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        properties[initialized] = .{
            .pattern = try alloc.dupe(u8, entry.key_ptr.*),
            .property = try parseAnonymousProperty(alloc, context, property_object),
        };
        initialized += 1;
    }
    return properties;
}

fn parseRequiredFields(alloc: std.mem.Allocator, required: std.json.Array) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, required.items.len);
    var initialized: usize = 0;
    errdefer {
        for (fields[0..initialized]) |field_name| alloc.free(field_name);
        alloc.free(fields);
    }

    for (required.items) |entry| {
        fields[initialized] = try alloc.dupe(u8, entry.string);
        initialized += 1;
    }
    return fields;
}

fn parseEnumValues(alloc: std.mem.Allocator, values: std.json.Array) ![][]const u8 {
    const enum_values = try alloc.alloc([]const u8, values.items.len);
    var initialized: usize = 0;
    errdefer {
        for (enum_values[0..initialized]) |enum_value| alloc.free(enum_value);
        alloc.free(enum_values);
    }

    for (values.items) |value| {
        enum_values[initialized] = try stringifyJsonValue(alloc, value);
        initialized += 1;
    }
    return enum_values;
}

fn parsePropertyVariants(alloc: std.mem.Allocator, context: SchemaContext, variants: std.json.Array) ![]DocumentProperty {
    const parsed = try alloc.alloc(DocumentProperty, variants.items.len);
    var initialized: usize = 0;
    errdefer {
        for (parsed[0..initialized]) |*variant| variant.deinit(alloc);
        alloc.free(parsed);
    }

    for (variants.items) |variant_value| {
        const variant_object = switch (variant_value) {
            .object => |object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const variant = try parseAnonymousProperty(alloc, context, variant_object);
        defer alloc.destroy(variant);
        parsed[initialized] = variant.*;
        initialized += 1;
    }
    return parsed;
}

fn parseDynamicTemplates(alloc: std.mem.Allocator, value: std.json.Value) ![]DynamicTemplate {
    return switch (value) {
        .object => |object| blk: {
            const templates = try alloc.alloc(DynamicTemplate, object.count());
            var initialized: usize = 0;
            errdefer {
                for (templates[0..initialized]) |*template| template.deinit(alloc);
                alloc.free(templates);
            }

            var it = object.iterator();
            while (it.next()) |entry| {
                templates[initialized] = try parseDynamicTemplate(alloc, entry.key_ptr.*, entry.value_ptr.*);
                initialized += 1;
            }
            break :blk templates;
        },
        .array => |array| blk: {
            const templates = try alloc.alloc(DynamicTemplate, array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (templates[0..initialized]) |*template| template.deinit(alloc);
                alloc.free(templates);
            }

            for (array.items) |item| {
                const name = if (item.object.get("name")) |name| switch (name) {
                    .string => |string| string,
                    else => "",
                } else "";
                templates[initialized] = try parseDynamicTemplate(alloc, name, item);
                initialized += 1;
            }
            break :blk templates;
        },
        else => unreachable,
    };
}

fn parseDynamicTemplate(alloc: std.mem.Allocator, default_name: []const u8, value: std.json.Value) !DynamicTemplate {
    const object = value.object;
    const mapping = object.get("mapping").?.object;
    const field_type = if (mapping.get("type")) |mapping_type|
        switch (mapping_type) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (field_type) |owned| alloc.free(owned);
    const analyzer = if (mapping.get("analyzer")) |analyzer_value|
        switch (analyzer_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (analyzer) |owned| alloc.free(owned);
    const match_mapping_type = if (object.get("match_mapping_type")) |match_mapping_type_value|
        switch (match_mapping_type_value) {
            .string => |name| try alloc.dupe(u8, name),
            .null => null,
            else => null,
        }
    else
        null;
    errdefer if (match_mapping_type) |owned| alloc.free(owned);

    return .{
        .name = try alloc.dupe(u8, default_name),
        .match_pattern = if (object.get("match")) |match| switch (match) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else if (object.get("match_pattern")) |match_pattern| switch (match_pattern) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .unmatch_pattern = if (object.get("unmatch")) |unmatch| switch (unmatch) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .path_match = if (object.get("path_match")) |path_match| switch (path_match) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .path_unmatch = if (object.get("path_unmatch")) |path_unmatch| switch (path_unmatch) {
            .string => |pattern| try alloc.dupe(u8, pattern),
            else => null,
        } else null,
        .match_mapping_type = match_mapping_type,
        .field_type = field_type,
        .analyzer = analyzer,
        .do_index = if (mapping.get("index")) |index| switch (index) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .store = if (mapping.get("store")) |store| switch (store) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .doc_values = if (mapping.get("doc_values")) |doc_values| switch (doc_values) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
        .include_in_all = if (mapping.get("include_in_all")) |include_in_all| switch (include_in_all) {
            .bool => |enabled| enabled,
            else => null,
        } else null,
    };
}

fn resolveDocumentSchema(
    schema: TableSchema,
    root: std.json.ObjectMap,
) !?DocumentSchema {
    if (schema.document_schemas.len == 0) return null;

    if (root.get("_type")) |type_value| {
        if (type_value == .null) return error.InvalidBatchRequest;
        const document_type = switch (type_value) {
            .string => |document_type| document_type,
            else => return error.InvalidBatchRequest,
        };
        return findDocumentSchema(schema.document_schemas, document_type) orelse return error.InvalidBatchRequest;
    }

    if (schema.default_type.len > 0) {
        if (findDocumentSchema(schema.document_schemas, schema.default_type)) |document_schema| return document_schema;
    }
    if (schema.document_schemas.len == 1) return schema.document_schemas[0];
    return error.InvalidBatchRequest;
}

fn findDocumentSchema(document_schemas: []const DocumentSchema, document_type: []const u8) ?DocumentSchema {
    for (document_schemas) |document_schema| {
        if (std.mem.eql(u8, document_schema.name, document_type)) return document_schema;
    }
    return null;
}

fn findDocumentProperty(properties: []const DocumentProperty, field_name: []const u8) ?DocumentProperty {
    for (properties) |property| {
        if (std.mem.eql(u8, property.name, field_name)) return property;
    }
    return null;
}

fn shouldIgnoreSchemaValidationField(field_name: []const u8) bool {
    return field_name.len > 0 and field_name[0] == '_';
}

fn fieldMatchesDynamicTemplates(dynamic_templates: []const DynamicTemplate, path: []const u8, value: std.json.Value) bool {
    const field_name = fieldNameFromPath(path);
    for (dynamic_templates) |dynamic_template| {
        if (dynamicTemplateMatches(dynamic_template, path, field_name, value)) return true;
    }
    return false;
}

fn dynamicTemplateMatches(
    dynamic_template: DynamicTemplate,
    path: []const u8,
    field_name: []const u8,
    value: std.json.Value,
) bool {
    if (dynamic_template.match_pattern) |match_pattern| {
        if (!globMatch(match_pattern, field_name)) return false;
    }
    if (dynamic_template.unmatch_pattern) |unmatch_pattern| {
        if (globMatch(unmatch_pattern, field_name)) return false;
    }
    if (dynamic_template.path_match) |path_match| {
        if (!globMatch(path_match, path)) return false;
    }
    if (dynamic_template.path_unmatch) |path_unmatch| {
        if (globMatch(path_unmatch, path)) return false;
    }
    if (dynamic_template.match_mapping_type) |match_mapping_type| {
        const inferred = inferDynamicTemplateMatchType(value) orelse return false;
        if (!std.mem.eql(u8, match_mapping_type, inferred)) return false;
    }
    return true;
}

fn inferDynamicTemplateMatchType(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| if (parseRfc3339ToNs(text) != null or isValidDate(text)) "date" else "string",
        .integer, .float, .number_string => "number",
        .bool => "boolean",
        .object => "object",
        else => null,
    };
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[last_dot + 1 ..];
}

fn validateDocumentFieldValueWithContext(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) !void {
    const composed_enforce_types = false;

    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        if (try context.rootRefGuard(value)) |*guard| {
            defer guard.release();
            try validateDocumentFieldValueWithContext(context, root_property.*, value, enforce_types);
        }
    }

    if (value.* == .null) return validateNullValueWithContext(context, property, value, enforce_types);

    if (property.all_of.len > 0) {
        for (property.all_of) |variant| try validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types);
    }

    if (property.any_of.len > 0) {
        var matched = false;
        for (property.any_of) |variant| {
            validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matched = true;
            break;
        }
        if (!matched) return error.InvalidBatchRequest;
    }

    if (property.one_of.len > 0) {
        var matches: usize = 0;
        for (property.one_of) |variant| {
            validateDocumentFieldValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matches += 1;
        }
        if (matches != 1) return error.InvalidBatchRequest;
    }

    if (property.not_schema) |not_schema| {
        if (validateDocumentFieldValueWithContext(context, not_schema.*, value, composed_enforce_types)) |_| {
            return error.InvalidBatchRequest;
        } else |_| {}
    }

    if (property.if_schema) |if_schema| {
        const matched = if (validateDocumentFieldValueWithContext(context, if_schema.*, value, composed_enforce_types)) |_| true else |_| false;
        if (matched) {
            if (property.then_schema) |then_schema| try validateDocumentFieldValueWithContext(context, then_schema.*, value, composed_enforce_types);
        } else {
            if (property.else_schema) |else_schema| try validateDocumentFieldValueWithContext(context, else_schema.*, value, composed_enforce_types);
        }
    }

    if (property.const_value) |const_value| {
        const rendered = try stringifyJsonValue(std.heap.page_allocator, value.*);
        defer std.heap.page_allocator.free(rendered);
        if (!std.mem.eql(u8, const_value, rendered)) return error.InvalidBatchRequest;
    }

    if (property.enum_values.len > 0) {
        const rendered = try stringifyJsonValue(std.heap.page_allocator, value.*);
        defer std.heap.page_allocator.free(rendered);

        var matched = false;
        for (property.enum_values) |enum_value| {
            if (std.mem.eql(u8, enum_value, rendered)) {
                matched = true;
                break;
            }
        }
        if (!matched) return error.InvalidBatchRequest;
    }

    if (property.pattern) |pattern| {
        const string_value = switch (value.*) {
            .string => |string| string,
            else => return error.InvalidBatchRequest,
        };
        if (!try regexMatch(pattern, string_value)) return error.InvalidBatchRequest;
    }

    if (property.format) |format| {
        const string_value = switch (value.*) {
            .string => |string| string,
            else => return error.InvalidBatchRequest,
        };
        try validateStringFormat(format, string_value);
    }

    if (property.min_length != null or property.max_length != null) {
        if (value.* == .string) {
            const codepoints = std.unicode.utf8CountCodepoints(value.string) catch return error.InvalidBatchRequest;
            if (property.min_length) |min_length| {
                if (codepoints < min_length) return error.InvalidBatchRequest;
            }
            if (property.max_length) |max_length| {
                if (codepoints > max_length) return error.InvalidBatchRequest;
            }
        } else if (property.field_type == null) {
            return error.InvalidBatchRequest;
        }
    }

    if (property.min_items != null or property.max_items != null) {
        if (value.* == .array) {
            if (property.min_items) |min_items| {
                if (value.array.items.len < min_items) return error.InvalidBatchRequest;
            }
            if (property.max_items) |max_items| {
                if (value.array.items.len > max_items) return error.InvalidBatchRequest;
            }
        } else if (property.item == null and property.field_type == null) {
            return error.InvalidBatchRequest;
        }
    }

    if (property.prefix_items.len > 0) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        const prefix_len = @min(property.prefix_items.len, array.items.len);
        for (property.prefix_items[0..prefix_len], array.items[0..prefix_len]) |prefix_item, item_value| {
            try validateDocumentFieldValueWithContext(context, prefix_item, &item_value, enforce_types);
        }
        if (property.additional_items_allowed != null and property.item == null) {
            if (!(property.additional_items_allowed orelse true) and array.items.len > prefix_len) {
                return error.InvalidBatchRequest;
            }
        }
    }

    if (property.unique_items) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        for (array.items, 0..) |item, i| {
            for (array.items[i + 1 ..]) |other| {
                if (jsonValueEqual(item, other)) return error.InvalidBatchRequest;
            }
        }
    }

    if (property.contains_schema != null or property.min_contains != null or property.max_contains != null) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        const contains_schema = property.contains_schema orelse return error.InvalidBatchRequest;
        var match_count: u64 = 0;
        for (array.items) |item_value| {
            validateDocumentFieldValueWithContext(context, contains_schema.*, &item_value, enforce_types) catch continue;
            match_count += 1;
        }
        const min_contains = property.min_contains orelse 1;
        if (match_count < min_contains) return error.InvalidBatchRequest;
        if (property.max_contains) |max_contains| {
            if (match_count > max_contains) return error.InvalidBatchRequest;
        }
    }

    if (property.unevaluated_items_allowed != null or property.unevaluated_items_schema != null) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        var evaluated_indices = std.AutoHashMapUnmanaged(usize, void).empty;
        defer evaluated_indices.deinit(context.alloc);
        try collectEvaluatedArrayIndices(context, property, value, enforce_types, &evaluated_indices, false);
        for (array.items, 0..) |item_value, index| {
            if (evaluated_indices.contains(index)) continue;
            if (property.unevaluated_items_schema) |unevaluated_items_schema| {
                try validateDocumentFieldValueWithContext(context, unevaluated_items_schema.*, &item_value, enforce_types);
                continue;
            }
            if (property.unevaluated_items_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
            }
        }
    }

    if (property.properties.len > 0 or
        property.pattern_properties.len > 0 or
        property.required_fields.len > 0 or
        property.additional_properties_allowed != null or
        property.additional_properties_schema != null or
        property.unevaluated_properties_allowed != null or
        property.unevaluated_properties_schema != null or
        property.property_names != null or
        property.dependent_required.len > 0 or
        property.dependent_schemas.len > 0)
    {
        const object = switch (value.*) {
            .object => |object| object,
            else => return error.InvalidBatchRequest,
        };
        try validateObjectCardinality(object, property.min_properties, property.max_properties);
        try validateRequiredFieldsPresent(object, property.required_fields);
        if (property.property_names) |property_names| try validatePropertyNames(context, object, property_names.*, enforce_types);
        try validateDependentRequired(object, property.dependent_required);
        try validateDependentSchemas(context, object, property.dependent_schemas, composed_enforce_types);
        var composition_evaluated_fields = std.StringHashMapUnmanaged(void).empty;
        defer composition_evaluated_fields.deinit(context.alloc);
        try collectComposedObjectFieldCoverage(context, property, object, enforce_types, &composition_evaluated_fields, false);
        var it = object.iterator();
        while (it.next()) |entry| {
            if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
            if (findDocumentProperty(property.properties, entry.key_ptr.*)) |child_property| {
                try validateDocumentFieldValueWithContext(context, child_property, entry.value_ptr, enforce_types);
                continue;
            }
            if (try validatePatternProperties(context, entry.key_ptr.*, entry.value_ptr, property.pattern_properties, enforce_types)) continue;
            if (property.additional_properties_schema) |additional_properties_schema| {
                try validateDocumentFieldValueWithContext(context, additional_properties_schema.*, entry.value_ptr, enforce_types);
                continue;
            }
            if (property.additional_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
            if (composition_evaluated_fields.contains(entry.key_ptr.*)) continue;
            if (property.unevaluated_properties_schema) |unevaluated_properties_schema| {
                try validateDocumentFieldValueWithContext(context, unevaluated_properties_schema.*, entry.value_ptr, enforce_types);
                continue;
            }
            if (property.unevaluated_properties_allowed) |allowed| {
                if (!allowed) return error.InvalidBatchRequest;
                continue;
            }
            if (enforce_types) return error.InvalidBatchRequest;
        }
    }

    if (property.item) |item| {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        if (property.min_items) |min_items| {
            if (array.items.len < min_items) return error.InvalidBatchRequest;
        }
        if (property.max_items) |max_items| {
            if (array.items.len > max_items) return error.InvalidBatchRequest;
        }
        const start_index = @min(property.prefix_items.len, array.items.len);
        for (array.items[start_index..]) |item_value| try validateDocumentFieldValueWithContext(context, item.*, &item_value, enforce_types);
    }

    const field_type = property.field_type orelse return;

    if (std.mem.eql(u8, field_type, "text") or
        std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "blob") or
        std.mem.eql(u8, field_type, "html") or
        std.mem.eql(u8, field_type, "search_as_you_type") or
        std.mem.eql(u8, field_type, "string"))
    {
        if (value.* != .string) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "integer"))
    {
        const numeric_value = parseJsonNumber(value.*) catch return error.InvalidBatchRequest;
        if ((property.integer_only or std.mem.eql(u8, field_type, "integer")) and !isIntegralJsonNumber(value.*, numeric_value)) {
            return error.InvalidBatchRequest;
        }
        if (property.minimum) |minimum| {
            if (numeric_value < minimum) return error.InvalidBatchRequest;
        }
        if (property.maximum) |maximum| {
            if (numeric_value > maximum) return error.InvalidBatchRequest;
        }
        if (property.exclusive_minimum) |exclusive_minimum| {
            if (numeric_value <= exclusive_minimum) return error.InvalidBatchRequest;
        }
        if (property.exclusive_maximum) |exclusive_maximum| {
            if (numeric_value >= exclusive_maximum) return error.InvalidBatchRequest;
        }
        if (property.multiple_of) |multiple_of| {
            if (!isMultipleOf(numeric_value, multiple_of)) return error.InvalidBatchRequest;
        }
        return;
    }
    if (std.mem.eql(u8, field_type, "boolean")) {
        if (value.* != .bool) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "null")) return error.InvalidBatchRequest;
    if (std.mem.eql(u8, field_type, "datetime")) {
        switch (value.*) {
            .string, .integer, .number_string => return,
            else => return error.InvalidBatchRequest,
        }
    }
    if (std.mem.eql(u8, field_type, "object")) {
        if (value.* != .object) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, field_type, "array")) {
        const array = switch (value.*) {
            .array => |array| array,
            else => return error.InvalidBatchRequest,
        };
        if (property.min_items) |min_items| {
            if (array.items.len < min_items) return error.InvalidBatchRequest;
        }
        if (property.max_items) |max_items| {
            if (array.items.len > max_items) return error.InvalidBatchRequest;
        }
        return;
    }
}

fn validateNullValueWithContext(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) !void {
    _ = enforce_types;
    const composed_enforce_types = false;
    const has_all_of = property.all_of.len > 0;

    if (has_all_of) {
        for (property.all_of) |variant| try validateNullValueWithContext(context, variant, value, composed_enforce_types);
    }

    if (property.any_of.len > 0) {
        for (property.any_of) |variant| {
            validateNullValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            return;
        }
        return error.InvalidBatchRequest;
    }

    if (property.one_of.len > 0) {
        var matches: usize = 0;
        for (property.one_of) |variant| {
            validateNullValueWithContext(context, variant, value, composed_enforce_types) catch continue;
            matches += 1;
        }
        if (matches != 1) return error.InvalidBatchRequest;
        return;
    }

    if (property.not_schema) |not_schema| {
        if (validateNullValueWithContext(context, not_schema.*, value, composed_enforce_types)) |_| {
            return error.InvalidBatchRequest;
        } else |_| {}
    }

    if (property.if_schema) |if_schema| {
        const matched = if (validateNullValueWithContext(context, if_schema.*, value, composed_enforce_types)) |_| true else |_| false;
        if (matched) {
            if (property.then_schema) |then_schema| {
                try validateNullValueWithContext(context, then_schema.*, value, composed_enforce_types);
                return;
            }
            return;
        }
        if (property.else_schema) |else_schema| {
            try validateNullValueWithContext(context, else_schema.*, value, composed_enforce_types);
            return;
        }
    }

    if (property.const_value) |const_value| {
        if (!std.mem.eql(u8, const_value, "null")) return error.InvalidBatchRequest;
        return;
    }

    if (property.enum_values.len > 0) {
        for (property.enum_values) |enum_value| {
            if (std.mem.eql(u8, enum_value, "null")) return;
        }
        return error.InvalidBatchRequest;
    }

    if (property.allows_null) return;
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "null")) return;
    }
    if (has_all_of and property.field_type == null and property.const_value == null and property.enum_values.len == 0) return;
    return error.InvalidBatchRequest;
}

fn isIntegralJsonNumber(value: std.json.Value, numeric_value: f64) bool {
    return switch (value) {
        .integer => true,
        .number_string => |text| blk: {
            _ = std.fmt.parseInt(i64, text, 10) catch break :blk false;
            break :blk true;
        },
        .float => std.math.floor(numeric_value) == numeric_value,
        else => false,
    };
}

fn validateTtlFieldValue(value: std.json.Value) !void {
    if (value == .null) return;
    _ = try parseTtlTimestampNs(value);
}

fn validateRequiredFieldsPresent(object: std.json.ObjectMap, required_fields: []const []const u8) !void {
    for (required_fields) |field_name| {
        if (!object.contains(field_name)) return error.InvalidBatchRequest;
    }
}

fn validateObjectCardinality(object: std.json.ObjectMap, min_properties: ?u64, max_properties: ?u64) !void {
    if (min_properties == null and max_properties == null) return;

    var count: u64 = 0;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        count += 1;
    }

    if (min_properties) |min_value| {
        if (count < min_value) return error.InvalidBatchRequest;
    }
    if (max_properties) |max_value| {
        if (count > max_value) return error.InvalidBatchRequest;
    }
}

fn makeRootDocumentProperty(document_schema: DocumentSchema) DocumentProperty {
    return .{
        .name = "",
        .field_type = "object",
        .min_properties = document_schema.min_properties,
        .max_properties = document_schema.max_properties,
        .required_fields = document_schema.required_fields,
        .properties = document_schema.properties,
        .pattern_properties = document_schema.pattern_properties,
        .additional_properties_allowed = document_schema.additional_properties_allowed,
        .additional_properties_schema = document_schema.additional_properties_schema,
        .dynamic_infer_types = document_schema.dynamic_infer_types,
        .unevaluated_properties_allowed = document_schema.unevaluated_properties_allowed,
        .unevaluated_properties_schema = document_schema.unevaluated_properties_schema,
        .property_names = document_schema.property_names,
        .dependent_required = document_schema.dependent_required,
        .dependent_schemas = document_schema.dependent_schemas,
        .any_of = document_schema.any_of,
        .one_of = document_schema.one_of,
        .all_of = document_schema.all_of,
        .not_schema = document_schema.not_schema,
        .if_schema = document_schema.if_schema,
        .then_schema = document_schema.then_schema,
        .else_schema = document_schema.else_schema,
    };
}

fn validatePropertyNames(
    context: *RuntimeValidationContext,
    object: std.json.ObjectMap,
    property_names: DocumentProperty,
    enforce_types: bool,
) anyerror!void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        const key_value: std.json.Value = .{ .string = entry.key_ptr.* };
        try validateDocumentFieldValueWithContext(context, property_names, &key_value, enforce_types);
    }
}

fn validatePatternProperties(
    context: *RuntimeValidationContext,
    field_name: []const u8,
    value: *const std.json.Value,
    pattern_properties: []const PatternProperty,
    enforce_types: bool,
) anyerror!bool {
    var matched = false;
    for (pattern_properties) |pattern_property| {
        if (!try regexMatch(pattern_property.pattern, field_name)) continue;
        try validateDocumentFieldValueWithContext(context, pattern_property.property.*, value, enforce_types);
        matched = true;
    }
    return matched;
}

fn validateDependentRequired(object: std.json.ObjectMap, dependent_required: []const DependentRequired) !void {
    for (dependent_required) |dependency| {
        if (!object.contains(dependency.name)) continue;
        try validateRequiredFieldsPresent(object, dependency.required_fields);
    }
}

fn validateDependentSchemas(
    context: *RuntimeValidationContext,
    object: std.json.ObjectMap,
    dependent_schemas: []const DependentSchema,
    enforce_types: bool,
) anyerror!void {
    for (dependent_schemas) |dependency| {
        if (!object.contains(dependency.name)) continue;
        const object_value: std.json.Value = .{ .object = object };
        try validateDocumentFieldValueWithContext(context, dependency.schema.*, &object_value, enforce_types);
    }
}

fn schemaMatchesValue(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
) bool {
    _ = enforce_types;
    validateDocumentFieldValueWithContext(context, property, value, false) catch return false;
    return true;
}

fn markAllObjectFieldsEvaluated(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
) anyerror!void {
    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        try evaluated_fields.put(alloc, entry.key_ptr.*, {});
    }
}

fn markDirectObjectFieldCoverage(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    object: std.json.ObjectMap,
    enforce_types: bool,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        const object_value: std.json.Value = .{ .object = object };
        if (try context.rootRefGuard(&object_value)) |*guard| {
            defer guard.release();
            try markDirectObjectFieldCoverage(context, root_property.*, object, enforce_types, evaluated_fields);
        }
        return;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        if (findDocumentProperty(property.properties, entry.key_ptr.*) != null) {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
            continue;
        }
        if (try validatePatternProperties(context, entry.key_ptr.*, entry.value_ptr, property.pattern_properties, enforce_types)) {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
            continue;
        }
    }

    if (property.additional_properties_schema != null or (property.additional_properties_allowed orelse false)) {
        var remaining = std.StringHashMapUnmanaged(void).empty;
        defer remaining.deinit(context.alloc);
        var object_it = object.iterator();
        while (object_it.next()) |entry| {
            if (shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
            if (evaluated_fields.contains(entry.key_ptr.*)) continue;
            if (property.additional_properties_schema) |schema| {
                try validateDocumentFieldValueWithContext(context, schema.*, entry.value_ptr, enforce_types);
            }
            try remaining.put(context.alloc, entry.key_ptr.*, {});
        }
        var remaining_it = remaining.iterator();
        while (remaining_it.next()) |entry| {
            try evaluated_fields.put(context.alloc, entry.key_ptr.*, {});
        }
    }
}

fn collectComposedObjectFieldCoverage(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    object: std.json.ObjectMap,
    enforce_types: bool,
    evaluated_fields: *std.StringHashMapUnmanaged(void),
    include_local_unevaluated: bool,
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        const object_value: std.json.Value = .{ .object = object };
        if (try context.rootRefGuard(&object_value)) |*guard| {
            defer guard.release();
            try collectComposedObjectFieldCoverage(context, root_property.*, object, enforce_types, evaluated_fields, include_local_unevaluated);
        }
        return;
    }

    try markDirectObjectFieldCoverage(context, property, object, enforce_types, evaluated_fields);

    const object_value: std.json.Value = .{ .object = object };
    for (property.all_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    for (property.any_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    for (property.one_of) |variant| {
        if (!schemaMatchesValue(context, variant, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, variant, object, enforce_types, evaluated_fields, true);
    }
    if (property.if_schema) |if_schema| {
        if (schemaMatchesValue(context, if_schema.*, &object_value, enforce_types)) {
            try collectComposedObjectFieldCoverage(context, if_schema.*, object, enforce_types, evaluated_fields, true);
            if (property.then_schema) |then_schema| {
                if (schemaMatchesValue(context, then_schema.*, &object_value, enforce_types)) {
                    try collectComposedObjectFieldCoverage(context, then_schema.*, object, enforce_types, evaluated_fields, true);
                }
            }
        } else if (property.else_schema) |else_schema| {
            if (schemaMatchesValue(context, else_schema.*, &object_value, enforce_types)) {
                try collectComposedObjectFieldCoverage(context, else_schema.*, object, enforce_types, evaluated_fields, true);
            }
        }
    }
    for (property.dependent_schemas) |dependency| {
        if (!object.contains(dependency.name)) continue;
        if (!schemaMatchesValue(context, dependency.schema.*, &object_value, enforce_types)) continue;
        try collectComposedObjectFieldCoverage(context, dependency.schema.*, object, enforce_types, evaluated_fields, true);
    }

    if (!include_local_unevaluated) return;

    if (property.unevaluated_properties_schema != null) {
        try markAllObjectFieldsEvaluated(context.alloc, object, evaluated_fields);
        return;
    }
    if (property.unevaluated_properties_allowed) |allowed| {
        if (allowed) try markAllObjectFieldsEvaluated(context.alloc, object, evaluated_fields);
    }
}

fn markDirectArrayIndicesEvaluated(
    alloc: std.mem.Allocator,
    property: DocumentProperty,
    array: std.json.Array,
    evaluated_indices: *std.AutoHashMapUnmanaged(usize, void),
) anyerror!void {
    const prefix_len = @min(property.prefix_items.len, array.items.len);
    for (0..prefix_len) |index| try evaluated_indices.put(alloc, index, {});
    if (property.item != null) {
        for (prefix_len..array.items.len) |index| try evaluated_indices.put(alloc, index, {});
    }
}

fn collectEvaluatedArrayIndices(
    context: *RuntimeValidationContext,
    property: DocumentProperty,
    value: *const std.json.Value,
    enforce_types: bool,
    evaluated_indices: *std.AutoHashMapUnmanaged(usize, void),
    include_local_unevaluated: bool,
) anyerror!void {
    if (property.root_ref) {
        const root_property = context.root_property orelse return error.InvalidBatchRequest;
        if (try context.rootRefGuard(value)) |*guard| {
            defer guard.release();
            try collectEvaluatedArrayIndices(context, root_property.*, value, enforce_types, evaluated_indices, include_local_unevaluated);
        }
        return;
    }

    const array = switch (value.*) {
        .array => |array| array,
        else => return error.InvalidBatchRequest,
    };

    try markDirectArrayIndicesEvaluated(context.alloc, property, array, evaluated_indices);

    if (property.contains_schema) |contains_schema| {
        for (array.items, 0..) |item_value, index| {
            if (!schemaMatchesValue(context, contains_schema.*, &item_value, enforce_types)) continue;
            try evaluated_indices.put(context.alloc, index, {});
        }
    }

    for (property.all_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    for (property.any_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    for (property.one_of) |variant| {
        if (!schemaMatchesValue(context, variant, value, enforce_types)) continue;
        try collectEvaluatedArrayIndices(context, variant, value, enforce_types, evaluated_indices, true);
    }
    if (property.if_schema) |if_schema| {
        if (schemaMatchesValue(context, if_schema.*, value, enforce_types)) {
            try collectEvaluatedArrayIndices(context, if_schema.*, value, enforce_types, evaluated_indices, true);
            if (property.then_schema) |then_schema| {
                if (schemaMatchesValue(context, then_schema.*, value, enforce_types)) {
                    try collectEvaluatedArrayIndices(context, then_schema.*, value, enforce_types, evaluated_indices, true);
                }
            }
        } else if (property.else_schema) |else_schema| {
            if (schemaMatchesValue(context, else_schema.*, value, enforce_types)) {
                try collectEvaluatedArrayIndices(context, else_schema.*, value, enforce_types, evaluated_indices, true);
            }
        }
    }

    if (!include_local_unevaluated) return;

    if (property.unevaluated_items_schema != null) {
        for (0..array.items.len) |index| try evaluated_indices.put(context.alloc, index, {});
        return;
    }
    if (property.unevaluated_items_allowed) |allowed| {
        if (allowed) {
            for (0..array.items.len) |index| try evaluated_indices.put(context.alloc, index, {});
        }
    }
}

fn jsonValueEqual(left: std.json.Value, right: std.json.Value) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;

    return switch (left) {
        .null => true,
        .bool => |bool_value| bool_value == right.bool,
        .integer => |integer| integer == right.integer,
        .float => |float_value| float_value == right.float,
        .number_string => |number_string| std.mem.eql(u8, number_string, right.number_string),
        .string => |string_value| std.mem.eql(u8, string_value, right.string),
        .array => |array| blk: {
            if (array.items.len != right.array.items.len) break :blk false;
            for (array.items, right.array.items) |left_item, right_item| {
                if (!jsonValueEqual(left_item, right_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (object.count() != right.object.count()) break :blk false;
            var it = object.iterator();
            while (it.next()) |entry| {
                const right_value = right.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValueEqual(entry.value_ptr.*, right_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn parseJsonNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |num| std.fmt.parseFloat(f64, num),
        else => error.InvalidNumber,
    };
}

fn isMultipleOf(value: f64, divisor: f64) bool {
    const quotient = value / divisor;
    return std.math.approxEqAbs(f64, quotient, @round(quotient), 1e-9);
}

fn regexMatch(pattern: []const u8, text: []const u8) !bool {
    return schema_regex.matches(std.heap.page_allocator, pattern, text) catch |err| switch (err) {
        error.InvalidRegex => error.InvalidSchemaUpdateRequest,
        else => err,
    };
}

fn parseTtlTimestampNs(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| blk: {
            if (integer < 0) return error.InvalidBatchRequest;
            break :blk std.math.cast(u64, integer) orelse return error.InvalidBatchRequest;
        },
        .number_string => |text| std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidBatchRequest,
        .string => |text| try parseTtlStringTimestampNs(text),
        else => error.InvalidBatchRequest,
    };
}

fn parseTtlStringTimestampNs(text: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidBatchRequest;
    if (std.fmt.parseInt(u64, trimmed, 10)) |ts| return ts else |_| {}
    return parseRfc3339ToNs(trimmed) orelse error.InvalidBatchRequest;
}

fn parseRfc3339ToNs(text: []const u8) ?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn validateStringFormat(format: []const u8, string_value: []const u8) !void {
    if (std.mem.eql(u8, format, "email")) {
        if (!isValidEmail(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "date-time")) {
        if (parseRfc3339ToNs(string_value) == null) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "date")) {
        if (!isValidDate(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uuid")) {
        if (!isValidUuid(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "ipv4")) {
        _ = std.Io.net.Ip4Address.parse(string_value, 0) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "ipv6")) {
        _ = std.Io.net.Ip6Address.parse(string_value, 0) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "hostname")) {
        if (!isValidHostname(string_value)) return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uri")) {
        _ = std.Uri.parse(string_value) catch return error.InvalidBatchRequest;
        return;
    }
    if (std.mem.eql(u8, format, "uri-reference")) {
        if (!isValidUriReference(string_value)) return error.InvalidBatchRequest;
        return;
    }
}

fn isValidEmail(value: []const u8) bool {
    if (value.len < 3 or std.mem.indexOfScalar(u8, value, ' ') != null) return false;
    const at_index = std.mem.indexOfScalar(u8, value, '@') orelse return false;
    if (at_index == 0 or at_index == value.len - 1) return false;
    if (std.mem.lastIndexOfScalar(u8, value, '@') != at_index) return false;
    const domain = value[at_index + 1 ..];
    const dot_index = std.mem.indexOfScalar(u8, domain, '.') orelse return false;
    if (dot_index == 0 or dot_index == domain.len - 1) return false;
    return true;
}

fn isValidDate(value: []const u8) bool {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return false;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return false;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return false;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return false;
    return civilDateTimeToNs(year, month, day, 0, 0, 0, 0) != null;
}

fn isValidHostname(value: []const u8) bool {
    if (value.len == 0 or value.len > 253) return false;
    var labels = std.mem.splitScalar(u8, value, '.');
    while (labels.next()) |label| {
        if (label.len == 0 or label.len > 63) return false;
        if (label[0] == '-' or label[label.len - 1] == '-') return false;
        for (label) |ch| {
            if (std.ascii.isAlphanumeric(ch) or ch == '-') continue;
            return false;
        }
    }
    return true;
}

fn isValidUriReference(value: []const u8) bool {
    if (value.len == 0) return true;
    if (std.Uri.parse(value)) |_| return true else |_| {}
    for (value) |ch| {
        if (std.ascii.isWhitespace(ch) or std.ascii.isControl(ch)) return false;
    }
    return true;
}

fn civilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    if (hour < 0 or hour > 23) return null;
    if (minute < 0 or minute > 59) return null;
    if (second < 0 or second > 60) return null;
    if (nanos >= std.time.ns_per_s) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn isValidUuid(value: []const u8) bool {
    if (value.len != 36) return false;
    for (value, 0..) |ch, i| {
        switch (i) {
            8, 13, 18, 23 => if (ch != '-') return false,
            else => if (!std.ascii.isHex(ch)) return false,
        }
    }
    return true;
}

test "parse schema and validate document writes" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"published\":{\"type\":\"boolean\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"published\":true,\"meta_status\":\"draft\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }}),
    );
}

test "relational embedded document schema is scoped to explicit json columns" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"json\",\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":true},\"dynamic_templates\":{\"metrics\":{\"path_match\":\"metrics.*\",\"mapping\":{\"type\":\"numeric\"}}}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(StorageMode.relational, parsed.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas[0].properties[1].embedded_dynamic_templates.len);

    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\",\"schema\":{\"type\":\"object\"}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"dynamic_templates\":{\"metrics\":{\"path_match\":\"metrics.*\",\"mapping\":{\"type\":\"numeric\"}}}}},\"required\":[\"id\"],\"additionalProperties\":false}}}}",
        ),
    );
}

test "parse dynamic template contract and validate selectors" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":[{\"name\":\"dates\",\"match\":\"*_at\",\"unmatch\":\"skip_*\",\"path_match\":\"meta.*\",\"path_unmatch\":\"meta.private.*\",\"match_mapping_type\":\"date\",\"mapping\":{\"type\":\"datetime\",\"analyzer\":\"keyword\",\"index\":false,\"store\":false,\"doc_values\":true,\"include_in_all\":false}}],\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), parsed.dynamic_templates.len);
    try std.testing.expectEqualStrings("dates", parsed.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("*_at", parsed.dynamic_templates[0].match_pattern.?);
    try std.testing.expectEqualStrings("skip_*", parsed.dynamic_templates[0].unmatch_pattern.?);
    try std.testing.expectEqualStrings("meta.*", parsed.dynamic_templates[0].path_match.?);
    try std.testing.expectEqualStrings("meta.private.*", parsed.dynamic_templates[0].path_unmatch.?);
    try std.testing.expectEqualStrings("date", parsed.dynamic_templates[0].match_mapping_type.?);
    try std.testing.expectEqualStrings("datetime", parsed.dynamic_templates[0].field_type.?);
    try std.testing.expectEqualStrings("keyword", parsed.dynamic_templates[0].analyzer.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].do_index.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].store.?);
    try std.testing.expectEqual(true, parsed.dynamic_templates[0].doc_values.?);
    try std.testing.expectEqual(false, parsed.dynamic_templates[0].include_in_all.?);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.created_at\":\"2026-01-03T00:00:00Z\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.skip_created_at\":\"2026-01-03T00:00:00Z\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta.created_at\":\"not-a-date\"}" }}),
    );
}

test "parse explicit field analyzer override" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\",\"x-antfly-types\":[\"text\",\"keyword\"],\"x-antfly-analyzer\":\"french\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas.len);
    const title = findDocumentProperty(parsed.document_schemas[0].properties, "title") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), title.antfly_types.len);
    try std.testing.expectEqualStrings("french", title.analyzer.?);
}

test "parse schema-present infer_types dynamic indexing opt-in" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"additionalProperties\":true,\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    const meta = findDocumentProperty(parsed.document_schemas[0].properties, "meta") orelse return error.TestExpectedEqual;
    try std.testing.expect(meta.additional_properties_allowed.?);
    try std.testing.expect(meta.dynamic_infer_types);
}

test "reject infer_types dynamic indexing without open additionalProperties" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"number\"},\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}}}",
        ),
    );
}

test "validate nested required fields and array items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"required\":[\"author\",\"tags\"],\"properties\":{\"author\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"active\":{\"type\":\"boolean\"}}},\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"keyword\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"name\":\"ann\",\"active\":true},\"tags\":[\"a\",\"b\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"active\":true},\"tags\":[\"a\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"author\":{\"name\":\"ann\"},\"tags\":[1]}" }}),
    );
}

test "validate enums numeric bounds and anyOf" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\",\"enum\":[\"draft\",\"published\"]},\"score\":{\"type\":\"numeric\",\"minimum\":0,\"maximum\":10},\"metric\":{\"anyOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"keyword\",\"enum\":[\"n/a\"]}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"n/a\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"published\",\"score\":0,\"metric\":3}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"archived\",\"score\":8,\"metric\":\"n/a\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":11,\"metric\":\"n/a\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"status\":\"draft\",\"score\":8,\"metric\":\"bad\"}" }}),
    );
}

test "validate exclusive numeric bounds and multipleOf" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"score\":{\"type\":\"numeric\",\"exclusiveMinimum\":0,\"exclusiveMaximum\":10,\"multipleOf\":0.5}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":5.5}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":0}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":10}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"score\":5.25}" }}),
    );
}

test "validate nullable and type-array fields" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"subtitle\":{\"type\":[\"text\",\"null\"]},\"score\":{\"type\":\"numeric\",\"nullable\":true},\"flag\":{\"type\":[\"boolean\"]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":null,\"score\":null,\"flag\":true}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":false}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":null,\"subtitle\":\"beta\",\"score\":1,\"flag\":true}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"subtitle\":\"beta\",\"score\":1,\"flag\":null}" }}),
    );
}

test "validate local defs and refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"metaField\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}}},\"scoreField\":{\"type\":\"numeric\",\"nullable\":true}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\"},\"meta\":{\"$ref\":\"#/$defs/metaField\"},\"score\":{\"$ref\":\"#/$defs/scoreField\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"status\":\"ready\"},\"score\":null}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":1,\"meta\":{\"status\":\"ready\"},\"score\":null}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"status\":1},\"score\":null}" }}),
    );
}

test "validate ref siblings and nested local defs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"titleField\":{\"type\":\"text\"},\"sharedText\":{\"type\":\"text\",\"minLength\":8}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/titleField\",\"minLength\":3},\"meta\":{\"type\":\"object\",\"$defs\":{\"sharedText\":{\"type\":\"text\",\"minLength\":4}},\"properties\":{\"note\":{\"$ref\":\"#/$defs/sharedText\",\"maxLength\":6}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"short\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"ab\",\"meta\":{\"note\":\"short\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"abc\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"note\":\"toolong\"}}" }}),
    );
}

test "validate recursive root refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[{\"name\":\"leaf\",\"children\":[]},{\"name\":\"branch\",\"children\":[{\"name\":\"twig\"}]}]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[{\"name\":1}]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"children\":[null]}" }}),
    );
}

test "validate recursive root refs with closure semantics" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"node\",\"enforce_types\":true,\"document_schemas\":{\"node\":{\"schema\":{\"type\":\"object\",\"required\":[\"name\"],\"properties\":{\"name\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^tag_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"children\":{\"type\":\"array\",\"items\":{\"$ref\":\"#\"}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":\"leaf\"},\"children\":[]},{\"name\":\"branch\",\"meta\":{\"tag_kind\":\"branch\",\"count\":1},\"children\":[{\"name\":\"twig\",\"meta\":{\"tag_kind\":\"twig\"}}]}]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"extra\":\"bad\"}]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"other\":\"bad\"},\"children\":[]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"name\":\"root\",\"meta\":{\"tag_kind\":\"oak\",\"count\":2},\"children\":[{\"name\":\"leaf\",\"meta\":{\"tag_kind\":1}}]}" }}),
    );
}

test "validate format and additionalItems" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"keyword\",\"format\":\"email\"},\"site\":{\"type\":\"keyword\",\"format\":\"uri\"},\"id\":{\"type\":\"keyword\",\"format\":\"uuid\"},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"additionalItems\":false},\"labels\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\"}],\"additionalItems\":{\"type\":\"keyword\",\"pattern\":\"^[a-z]+$\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\",\"beta\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"bad\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"not a uri\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"bad-uuid\",\"coords\":[\"point\",1],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1,2],\"labels\":[\"seed\",\"alpha\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"email\":\"a@example.com\",\"site\":\"https://example.com/docs\",\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"coords\":[\"point\",1],\"labels\":[\"seed\",1]}" }}),
    );
}

test "validate broader string formats" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"published_at\":{\"type\":\"keyword\",\"format\":\"date-time\"},\"birthday\":{\"type\":\"keyword\",\"format\":\"date\"},\"v4\":{\"type\":\"keyword\",\"format\":\"ipv4\"},\"v6\":{\"type\":\"keyword\",\"format\":\"ipv6\"},\"host\":{\"type\":\"keyword\",\"format\":\"hostname\"},\"ref\":{\"type\":\"keyword\",\"format\":\"uri-reference\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-13-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"999.1.1.1\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"invalid\",\"host\":\"api.example.com\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"-bad-host\",\"ref\":\"/docs/intro\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"published_at\":\"2024-01-02T03:04:05Z\",\"birthday\":\"2024-01-02\",\"v4\":\"192.168.1.10\",\"v6\":\"2001:db8::1\",\"host\":\"api.example.com\",\"ref\":\"/docs bad\"}" }}),
    );
}

test "validate unevaluated properties and items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"meta\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}},\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"}],\"unevaluatedItems\":{\"type\":\"numeric\"}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",1,2]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":1},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"ok\"},\"coords\":[\"point\",\"bad\"]}" }}),
    );
}

test "validate unevaluated composition coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"kind\":{\"type\":\"keyword\"}}},{\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"properties\":{\"title\":{\"type\":\"text\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}],\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"line\",1,2]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"extra\":\"bad\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\",\"slug\":\"bad\"},\"coords\":[\"point\",1]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"title\":\"alpha\"},\"coords\":[\"point\",1,2]}" }}),
    );
}

test "validate root unevaluated properties in write loop" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"unevaluatedProperties\":{\"type\":\"keyword\"}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":1}" }}),
    );
}

test "validate conditional and dependency unevaluated coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"if\":{\"properties\":{\"kind\":{\"const\":\"story\"}}},\"then\":{\"required\":[\"slug\"],\"properties\":{\"slug\":{\"type\":\"keyword\"}}},\"else\":{\"required\":[\"rating\"],\"properties\":{\"rating\":{\"type\":\"numeric\"}}},\"dependentSchemas\":{\"kind\":{\"properties\":{\"details\":{\"type\":\"text\"}}}},\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"score\",\"rating\":5,\"details\":\"body\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"details\":\"body\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"score\",\"details\":\"body\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"body\",\"extra\":\"bad\"}" }}),
    );
}

test "validate anyOf and oneOf branch evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"}},\"allOf\":[{\"properties\":{\"meta\":{\"type\":\"object\",\"anyOf\":[{\"properties\":{\"mode\":{\"const\":\"alpha\"},\"a\":{\"type\":\"keyword\"}}},{\"properties\":{\"mode\":{\"const\":\"beta\"},\"b\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false}}},{\"properties\":{\"choice\":{\"type\":\"object\",\"oneOf\":[{\"properties\":{\"mode\":{\"const\":\"left\"},\"left\":{\"type\":\"keyword\"}},\"required\":[\"mode\",\"left\"]},{\"properties\":{\"mode\":{\"const\":\"right\"},\"right\":{\"type\":\"numeric\"}},\"required\":[\"mode\",\"right\"]}],\"unevaluatedProperties\":false}}}],\"unevaluatedProperties\":false}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"b\":3},\"choice\":{\"mode\":\"right\",\"right\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"b\":3},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"beta\",\"a\":\"oops\"},\"choice\":{\"mode\":\"right\",\"right\":9}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\"},\"choice\":{\"mode\":\"left\",\"right\":9}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"meta\":{\"mode\":\"alpha\",\"a\":\"ok\",\"extra\":\"bad\"},\"choice\":{\"mode\":\"left\",\"left\":\"x\"}}" }}),
    );
}

test "validate anyOf and oneOf array evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"coords\":{\"type\":\"array\",\"anyOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"point\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false},{\"minItems\":3,\"prefixItems\":[{\"const\":\"line\"},{\"type\":\"numeric\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]},\"choice\":{\"type\":\"array\",\"oneOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"left\"},{\"type\":\"keyword\"}],\"unevaluatedItems\":false},{\"minItems\":2,\"prefixItems\":[{\"const\":\"right\"},{\"type\":\"numeric\"}],\"unevaluatedItems\":false}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",\"ok\"]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"line\",1,2],\"choice\":[\"right\",9]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1,2],\"choice\":[\"left\",\"ok\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"line\",1],\"choice\":[\"right\",9]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"left\",9]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"choice\":[\"right\",9,10]}" }}),
    );
}

test "validate composed contains-driven array evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"series\":{\"type\":\"array\",\"allOf\":[{\"minItems\":2,\"prefixItems\":[{\"const\":\"set\"}]},{\"contains\":{\"type\":\"numeric\",\"minimum\":10},\"minContains\":1}],\"unevaluatedItems\":false},\"selector\":{\"type\":\"array\",\"anyOf\":[{\"contains\":{\"const\":\"hot\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"cold\"},\"minContains\":1,\"unevaluatedItems\":false}]},\"exclusive\":{\"type\":\"array\",\"oneOf\":[{\"contains\":{\"const\":\"left\"},\"minContains\":1,\"unevaluatedItems\":false},{\"contains\":{\"const\":\"right\"},\"minContains\":1,\"unevaluatedItems\":false}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",10,11],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"cold\"],\"exclusive\":[\"right\"]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",10,1],\"selector\":[\"hot\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"warm\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\",\"cold\"],\"exclusive\":[\"left\"]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"series\":[\"set\",12],\"selector\":[\"hot\"],\"exclusive\":[\"left\",\"right\"]}" }}),
    );
}

test "validate composed pattern and additional properties evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},{\"properties\":{\"count\":{\"type\":\"numeric\"}}}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},{\"additionalProperties\":{\"type\":\"numeric\"}}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}],\"unevaluatedProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true,\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"name_primary\":\"alpha\",\"score\":9}}" }}),
    );
}

test "validate composed ref closure evaluation coverage" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"meta_patterns\":{\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"}}},\"meta_count\":{\"properties\":{\"count\":{\"type\":\"numeric\"}}},\"choice_flags\":{\"patternProperties\":{\"^flag_[a-z]+$\":{\"type\":\"boolean\"}}},\"choice_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"}},\"exclusive_names\":{\"patternProperties\":{\"^name_[a-z]+$\":{\"type\":\"text\"}},\"unevaluatedProperties\":false},\"exclusive_numbers\":{\"additionalProperties\":{\"type\":\"numeric\"},\"unevaluatedProperties\":false}},\"properties\":{\"meta\":{\"type\":\"object\",\"allOf\":[{\"$ref\":\"#/$defs/meta_patterns\"},{\"$ref\":\"#/$defs/meta_count\"}],\"unevaluatedProperties\":false},\"choice\":{\"type\":\"object\",\"anyOf\":[{\"$ref\":\"#/$defs/choice_flags\"},{\"$ref\":\"#/$defs/choice_numbers\"}],\"unevaluatedProperties\":false},\"exclusive\":{\"type\":\"object\",\"oneOf\":[{\"$ref\":\"#/$defs/exclusive_names\"},{\"$ref\":\"#/$defs/exclusive_numbers\"}],\"unevaluatedProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"score\":7},\"exclusive\":{\"score\":9}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"other\":\"bad\"},\"choice\":{\"flag_enabled\":true},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"meta\":{\"meta_title\":\"ok\",\"count\":2},\"choice\":{\"flag_enabled\":\"bad\"},\"exclusive\":{\"name_primary\":\"alpha\"}}" }}),
    );
}

test "validate nullable composed refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"nullable_keyword\":{\"type\":[\"keyword\",\"null\"]},\"null_or_x\":{\"anyOf\":[{\"const\":null},{\"type\":\"keyword\",\"enum\":[\"x\"]}]}} ,\"properties\":{\"maybe\":{\"allOf\":[{\"$ref\":\"#/$defs/nullable_keyword\"},{\"$ref\":\"#/$defs/null_or_x\"}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":null}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":\"x\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"maybe\":\"y\"}" }}),
    );
}

test "validate ttl field values and schema bindings" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(std.testing.allocator, "{\"ttl_duration_ns\":1,\"ttl_field\":\"\"}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchema(
            std.testing.allocator,
            "{\"ttl_duration_ns\":1,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"keyword\"}}}}}}",
        ),
    );

    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"ttl_duration_ns\":1,\"ttl_field\":\"expires_at\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"expires_at\":{\"type\":\"datetime\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":1700000000000000000}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":\"2024-01-02T03:04:05Z\"}" }});
    try std.testing.expectEqual(
        @as(?u64, 1_700_000_000_000_000_000),
        try documentTtlTimestampNs(std.testing.allocator, parsed, "{\"expires_at\":1700000000000000000}"),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"expires_at\":\"not-a-time\"}" }}),
    );
}

test "validate escaped ref tokens and direct fragment refs" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"$defs\":{\"slash/name\":{\"type\":\"text\"},\"tilde~name\":{\"type\":\"keyword\"}},\"properties\":{\"title\":{\"$ref\":\"#/$defs/slash~1name\"},\"kind\":{\"$ref\":\"#/$defs/tilde~0name\"},\"meta\":{\"type\":\"object\",\"$defs\":{\"local/name\":{\"type\":\"text\"}},\"properties\":{\"note\":{\"$ref\":\"#/properties/meta/$defs/local~1name\"},\"shadow\":{\"$ref\":\"#/properties/title\"}},\"required\":[\"note\",\"shadow\"]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":1,\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":true,\"meta\":{\"note\":\"short\",\"shadow\":\"again\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"kind\":\"ready\",\"meta\":{\"note\":\"short\",\"shadow\":1}}" }}),
    );
}

test "validate oneOf allOf pattern and item cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"sku\":{\"type\":\"keyword\",\"pattern\":\"^[A-Z]{3}-[0-9]{2}$\"},\"tags\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"keyword\"}},\"code\":{\"oneOf\":[{\"type\":\"keyword\",\"enum\":[\"A\"]},{\"type\":\"keyword\",\"enum\":[\"B\"]}]},\"score\":{\"allOf\":[{\"type\":\"numeric\",\"minimum\":0},{\"type\":\"numeric\",\"maximum\":5}]}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"bad\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\",\"y\",\"z\"],\"code\":\"A\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"C\",\"score\":4}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"sku\":\"ABC-12\",\"tags\":[\"x\"],\"code\":\"A\",\"score\":8}" }}),
    );
}

test "validate string length and object cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"minProperties\":2,\"maxProperties\":3,\"properties\":{\"title\":{\"type\":\"text\",\"minLength\":3,\"maxLength\":5},\"meta\":{\"type\":\"object\",\"minProperties\":1,\"maxProperties\":2,\"properties\":{\"a\":{\"type\":\"keyword\"},\"b\":{\"type\":\"keyword\"},\"c\":{\"type\":\"keyword\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"hi\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alphabet\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\",\"c\":\"z\"}}" }}),
    );
}

test "validate root conditionals not and unique items" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"if\":{\"required\":[\"kind\"],\"properties\":{\"kind\":{\"enum\":[\"story\"]}}},\"then\":{\"required\":[\"headline\"]},\"else\":{\"required\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\",\"enum\":[\"story\",\"note\"]},\"headline\":{\"type\":\"text\"},\"slug\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"array\",\"uniqueItems\":true,\"items\":{\"type\":\"keyword\"}},\"status\":{\"type\":\"keyword\",\"not\":{\"enum\":[\"archived\"]}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"tags\":[\"a\",\"b\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"a\"],\"status\":\"draft\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"headline\":\"alpha\",\"tags\":[\"a\",\"b\"],\"status\":\"archived\"}" }}),
    );
}

test "validate property names and dependent required" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"dependentRequired\":{\"kind\":[\"slug\"]},\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\",\"propertyNames\":{\"pattern\":\"^meta_[a-z]+$\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"attrs\":{\"meta_color\":\"red\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"attrs\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"slug\":\"alpha\",\"attrs\":{\"bad\":\"red\"}}" }}),
    );
}

test "validate dependent schemas" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependentSchemas\":{\"kind\":{\"required\":[\"slug\"],\"properties\":{\"kind\":{\"const\":\"story\"},\"slug\":{\"type\":\"keyword\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"details\":\"ok\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"details\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"details\":\"ok\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"note\",\"slug\":\"alpha\",\"details\":\"ok\"}" }}),
    );
}

test "validate legacy dependencies keyword" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"kind\":{\"type\":\"keyword\"},\"slug\":{\"type\":\"keyword\"},\"mode\":{\"type\":\"keyword\"},\"details\":{\"type\":\"text\"}},\"dependencies\":{\"kind\":[\"slug\"],\"mode\":{\"required\":[\"details\"],\"properties\":{\"mode\":{\"const\":\"long\"},\"details\":{\"type\":\"text\"}}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\",\"details\":\"ok\"}" }});
    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"details\":\"ok\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"mode\":\"long\",\"details\":\"ok\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"long\"}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"kind\":\"story\",\"slug\":\"alpha\",\"mode\":\"short\",\"details\":\"ok\"}" }}),
    );
}

test "validate additional properties" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"title\":{\"type\":\"text\"},\"meta\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"keyword\"}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":\"x\",\"b\":\"y\"}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"body\":\"unexpected\",\"meta\":{\"a\":\"x\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"title\":\"alpha\",\"meta\":{\"a\":1}}" }}),
    );
}

test "validate contains and contains cardinality" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"tags\":{\"type\":\"array\",\"contains\":{\"type\":\"keyword\",\"const\":\"hot\"},\"minContains\":1,\"maxContains\":2},\"scores\":{\"type\":\"array\",\"contains\":{\"type\":\"numeric\",\"minimum\":10}}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\",\"warm\"],\"scores\":[1,10,20]}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"warm\"],\"scores\":[10]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\",\"hot\",\"hot\"],\"scores\":[10]}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"tags\":[\"hot\"],\"scores\":[1,2,3]}" }}),
    );
}

test "validate prefix items and pattern properties" {
    var parsed = try parseSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"coords\":{\"type\":\"array\",\"prefixItems\":[{\"type\":\"keyword\",\"const\":\"point\"},{\"type\":\"numeric\"}],\"items\":{\"type\":\"numeric\"}},\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^meta_[a-z]+$\":{\"type\":\"keyword\"},\"^flag_[a-z]+$\":{\"type\":\"boolean\"}},\"additionalProperties\":false}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1,2,3],\"meta\":{\"meta_color\":\"red\",\"flag_ready\":true}}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[1,1,2],\"meta\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",\"bad\"],\"meta\":{\"meta_color\":\"red\"}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"meta\":{\"meta_color\":1}}" }}),
    );
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateWritesAgainstSchema(std.testing.allocator, parsed, &.{.{ .value = "{\"coords\":[\"point\",1],\"meta\":{\"other\":\"x\"}}" }}),
    );
}

test "validate generic json schema object" {
    try validateJsonSchemaJson(std.testing.allocator,
        \\{
        \\  "type": "object",
        \\  "required": ["name", "count"],
        \\  "properties": {
        \\    "name": { "type": "string", "pattern": "^[a-z]+$" },
        \\    "count": { "type": "integer", "minimum": 1 }
        \\  },
        \\  "additionalProperties": false
        \\}
    ,
        \\{"name":"alpha","count":2}
    );
}

test "validate generic json schema rejects integer mismatch" {
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateJsonSchemaJson(std.testing.allocator,
            \\{
            \\  "type": "object",
            \\  "properties": {
            \\    "count": { "type": "integer" }
            \\  }
            \\}
        ,
            \\{"count":2.5}
        ),
    );
}
