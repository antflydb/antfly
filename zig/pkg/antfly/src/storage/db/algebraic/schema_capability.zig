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
const Allocator = std.mem.Allocator;
const index_mod = @import("index.zig");
const schema_mod = @import("../../../schema/mod.zig");

pub const FieldRole = enum {
    group,
    measure,
    time,
};

pub const FieldCapability = struct {
    document_type: []u8,
    name: []u8,
    path: []u8,
    scalar_type: []u8,
    role: FieldRole,
    bounded: bool = true,
    dynamic_source: bool = false,

    pub fn deinit(self: *FieldCapability, alloc: Allocator) void {
        alloc.free(self.document_type);
        alloc.free(self.name);
        alloc.free(self.path);
        alloc.free(self.scalar_type);
        self.* = undefined;
    }
};

pub const Plan = struct {
    schema_version: u32 = 0,
    fields: []FieldCapability = &.{},
    skipped_dynamic_fields: u32 = 0,
    skipped_complex_fields: u32 = 0,
    skipped_unbounded_fields: u32 = 0,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.fields) |*field| field.deinit(alloc);
        if (self.fields.len > 0) alloc.free(self.fields);
        self.* = undefined;
    }
};

pub const ChangeImpact = struct {
    old_schema_version: u32 = 0,
    new_schema_version: u32 = 0,
    added_fields: u32 = 0,
    removed_fields: u32 = 0,
    changed_type_fields: u32 = 0,
    compatible_additive: bool = true,
    requires_rebuild: bool = false,
};

pub fn compilePlanAlloc(alloc: Allocator, schema: schema_mod.ParsedTableSchema) !Plan {
    var fields = std.ArrayListUnmanaged(FieldCapability).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(alloc);
        fields.deinit(alloc);
    }
    var skipped_dynamic_fields: u32 = 0;
    var skipped_complex_fields: u32 = 0;
    var skipped_unbounded_fields: u32 = 0;

    for (schema.document_schemas) |document_schema| {
        if (document_schema.additional_properties_allowed orelse false) {
            skipped_dynamic_fields += 1;
            skipped_unbounded_fields += 1;
        }
        if (document_schema.additional_properties_schema != null or document_schema.pattern_properties.len > 0 or document_schema.dynamic_infer_types) {
            skipped_dynamic_fields += 1;
            skipped_unbounded_fields += 1;
        }
        for (document_schema.properties) |property| {
            try collectPropertyCapabilities(
                alloc,
                document_schema.name,
                property.name,
                property,
                &fields,
                &skipped_dynamic_fields,
                &skipped_complex_fields,
                &skipped_unbounded_fields,
            );
        }
    }

    return .{
        .schema_version = schema.version,
        .fields = try fields.toOwnedSlice(alloc),
        .skipped_dynamic_fields = skipped_dynamic_fields,
        .skipped_complex_fields = skipped_complex_fields,
        .skipped_unbounded_fields = skipped_unbounded_fields,
    };
}

pub fn configJsonFromSchemaJsonAlloc(alloc: Allocator, table_name: []const u8, schema_json: []const u8) ![]u8 {
    var parsed = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    return try configJsonFromPlanAlloc(alloc, table_name, plan);
}

pub fn configJsonFromPlanAlloc(alloc: Allocator, table_name: []const u8, plan: Plan) ![]u8 {
    const fingerprint = try capabilityFingerprintAlloc(alloc, plan);
    defer alloc.free(fingerprint);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, "version");
    try out.appendSlice(alloc, ":2,");
    try appendJsonString(alloc, &out, "table");
    try out.append(alloc, ':');
    try appendJsonString(alloc, &out, table_name);
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "schema_version");
    try appendFmt(alloc, &out, ":{d}", .{plan.schema_version});
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "capability_fingerprint");
    try out.append(alloc, ':');
    try appendJsonString(alloc, &out, fingerprint);
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "capability_lifecycle_status");
    try out.append(alloc, ':');
    try appendJsonString(alloc, &out, "current");
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "capability_change_added_fields");
    try out.appendSlice(alloc, ":0,");
    try appendJsonString(alloc, &out, "capability_change_removed_fields");
    try out.appendSlice(alloc, ":0,");
    try appendJsonString(alloc, &out, "capability_change_changed_type_fields");
    try out.appendSlice(alloc, ":0");
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "skipped_dynamic_fields");
    try appendFmt(alloc, &out, ":{d}", .{plan.skipped_dynamic_fields});
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "skipped_complex_fields");
    try appendFmt(alloc, &out, ":{d}", .{plan.skipped_complex_fields});
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "skipped_unbounded_fields");
    try appendFmt(alloc, &out, ":{d}", .{plan.skipped_unbounded_fields});

    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "group_fields");
    try out.append(alloc, ':');
    try appendFieldArray(alloc, &out, plan, .group);

    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "measure_fields");
    try out.append(alloc, ':');
    try appendFieldArray(alloc, &out, plan, .measure);

    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "time_fields");
    try out.append(alloc, ':');
    try appendFieldArray(alloc, &out, plan, .time);

    try out.appendSlice(alloc, ",");
    try appendJsonString(alloc, &out, "laws");
    try out.appendSlice(alloc, ":[");
    try appendLawConfig(alloc, &out, "count", "count", "group", true);
    try out.append(alloc, ',');
    try appendLawConfig(alloc, &out, "sum", "sum", "group", true);
    try out.append(alloc, ',');
    try appendLawConfig(alloc, &out, "avg", "avg", "group", true);
    try out.append(alloc, ',');
    try appendLawConfig(alloc, &out, "min", "min", "lattice", false);
    try out.append(alloc, ',');
    try appendLawConfig(alloc, &out, "max", "max", "lattice", false);
    try out.appendSlice(alloc, "],");
    try appendJsonString(alloc, &out, "joins");
    try out.appendSlice(alloc, ":[],");
    try appendJsonString(alloc, &out, "adaptive");
    try out.appendSlice(alloc, ":{\"observe\":true,\"lazy_materialization\":false,\"dematerialization\":false,\"min_observations\":3},");
    try appendJsonString(alloc, &out, "materializations");
    try out.appendSlice(alloc, ":[]}");

    return try out.toOwnedSlice(alloc);
}

fn appendLawConfig(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    name: []const u8,
    id: []const u8,
    structure: []const u8,
    invertible: bool,
) !void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "name");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, "id");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, id);
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, "structure");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, structure);
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, "invertible");
    try out.appendSlice(alloc, if (invertible) ":true" else ":false");
    try out.append(alloc, '}');
}

pub fn capabilityFingerprintAlloc(alloc: Allocator, plan: Plan) ![]u8 {
    var canonical = std.ArrayListUnmanaged(u8).empty;
    defer canonical.deinit(alloc);
    try appendFmt(alloc, &canonical, "v:{d}|", .{plan.schema_version});
    for (plan.fields) |field| {
        try appendFmt(alloc, &canonical, "{s}:{s}:{s}:{s}:{s}|", .{
            @tagName(field.role),
            field.document_type,
            field.name,
            field.path,
            field.scalar_type,
        });
    }
    try appendFmt(alloc, &canonical, "skip:{d}:{d}:{d}", .{
        plan.skipped_dynamic_fields,
        plan.skipped_complex_fields,
        plan.skipped_unbounded_fields,
    });
    const hash = std.hash.Wyhash.hash(0, canonical.items);
    return try std.fmt.allocPrint(alloc, "{x:0>16}", .{hash});
}

fn appendFmt(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const text = try std.fmt.allocPrint(alloc, fmt, args);
    defer alloc.free(text);
    try out.appendSlice(alloc, text);
}

pub fn classifyChange(old: Plan, new: Plan) ChangeImpact {
    var impact: ChangeImpact = .{
        .old_schema_version = old.schema_version,
        .new_schema_version = new.schema_version,
    };

    for (old.fields) |old_field| {
        if (findExactField(new.fields, old_field) != null) continue;
        if (findIdentityField(new.fields, old_field) != null) {
            impact.changed_type_fields += 1;
        } else {
            impact.removed_fields += 1;
        }
    }

    for (new.fields) |new_field| {
        if (findIdentityField(old.fields, new_field) == null) impact.added_fields += 1;
    }

    impact.requires_rebuild = impact.removed_fields > 0 or impact.changed_type_fields > 0;
    impact.compatible_additive = !impact.requires_rebuild;
    return impact;
}

fn findExactField(fields: []const FieldCapability, needle: FieldCapability) ?FieldCapability {
    for (fields) |field| {
        if (sameField(field, needle)) return field;
    }
    return null;
}

fn findIdentityField(fields: []const FieldCapability, needle: FieldCapability) ?FieldCapability {
    for (fields) |field| {
        if (sameFieldIdentity(field, needle)) return field;
    }
    return null;
}

fn sameField(lhs: FieldCapability, rhs: FieldCapability) bool {
    return sameFieldIdentity(lhs, rhs) and std.mem.eql(u8, lhs.scalar_type, rhs.scalar_type);
}

fn sameFieldIdentity(lhs: FieldCapability, rhs: FieldCapability) bool {
    return lhs.role == rhs.role and
        std.mem.eql(u8, lhs.document_type, rhs.document_type) and
        std.mem.eql(u8, lhs.name, rhs.name) and
        std.mem.eql(u8, lhs.path, rhs.path);
}

fn appendFieldArray(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: Plan,
    role: FieldRole,
) !void {
    try out.append(alloc, '[');
    var emitted = false;
    for (plan.fields, 0..) |field, i| {
        if (field.role != role) continue;
        if (fieldAlreadyEmitted(plan.fields[0..i], field, role)) continue;
        if (emitted) try out.append(alloc, ',');
        emitted = true;
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.name);
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "path");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.path);
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.scalar_type);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
}

fn fieldAlreadyEmitted(fields: []const FieldCapability, field: FieldCapability, role: FieldRole) bool {
    for (fields) |existing| {
        if (existing.role != role) continue;
        if (!std.mem.eql(u8, existing.name, field.name)) continue;
        if (!std.mem.eql(u8, existing.path, field.path)) continue;
        if (!std.mem.eql(u8, existing.scalar_type, field.scalar_type)) continue;
        return true;
    }
    return false;
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn collectPropertyCapabilities(
    alloc: Allocator,
    document_type: []const u8,
    path: []const u8,
    property: anytype,
    fields: *std.ArrayListUnmanaged(FieldCapability),
    skipped_dynamic_fields: *u32,
    skipped_complex_fields: *u32,
    skipped_unbounded_fields: *u32,
) !void {
    if (property.antfly_index != null and !property.antfly_index.?) return;

    if (property.additional_properties_allowed orelse false) {
        skipped_dynamic_fields.* += 1;
        skipped_unbounded_fields.* += 1;
    }
    if (property.additional_properties_schema != null or property.pattern_properties.len > 0 or property.dynamic_infer_types) {
        skipped_dynamic_fields.* += 1;
        skipped_unbounded_fields.* += 1;
    }
    if (property.any_of.len > 0 or property.one_of.len > 0 or property.all_of.len > 0 or property.not_schema != null or property.if_schema != null) skipped_complex_fields.* += 1;

    if (property.item != null) {
        skipped_complex_fields.* += 1;
        return;
    }

    if (property.properties.len > 0) {
        for (property.properties) |child| {
            const child_path = try appendPath(alloc, path, child.name);
            defer alloc.free(child_path);
            try collectPropertyCapabilities(
                alloc,
                document_type,
                child_path,
                child,
                fields,
                skipped_dynamic_fields,
                skipped_complex_fields,
                skipped_unbounded_fields,
            );
        }
        return;
    }

    const scalar = scalarType(property) orelse {
        skipped_complex_fields.* += 1;
        return;
    };
    const field_name = fieldNameFromPath(path);
    if (isGroupType(scalar)) {
        try appendCapability(alloc, fields, document_type, field_name, path, scalar, .group);
    }
    if (isMeasureType(scalar)) {
        try appendCapability(alloc, fields, document_type, field_name, path, scalar, .measure);
    }
    if (isTimeType(scalar, property.format)) {
        try appendCapability(alloc, fields, document_type, field_name, path, "datetime", .time);
    }
}

fn appendCapability(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(FieldCapability),
    document_type: []const u8,
    name: []const u8,
    path: []const u8,
    scalar_type_value: []const u8,
    role: FieldRole,
) !void {
    try fields.append(alloc, .{
        .document_type = try alloc.dupe(u8, document_type),
        .name = try alloc.dupe(u8, name),
        .path = try alloc.dupe(u8, path),
        .scalar_type = try alloc.dupe(u8, scalar_type_value),
        .role = role,
        .bounded = true,
        .dynamic_source = false,
    });
}

fn scalarType(property: anytype) ?[]const u8 {
    const field_type = property.field_type orelse {
        if (property.const_value != null or property.enum_values.len > 0) return "string";
        return null;
    };
    if (std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "string"))
    {
        return "string";
    }
    if (std.mem.eql(u8, field_type, "boolean") or std.mem.eql(u8, field_type, "bool")) return "boolean";
    if (std.mem.eql(u8, field_type, "datetime")) return "datetime";
    if (std.mem.eql(u8, field_type, "integer") or property.integer_only) return "integer";
    if (std.mem.eql(u8, field_type, "numeric") or std.mem.eql(u8, field_type, "number")) return "number";
    return null;
}

fn isGroupType(scalar_type_value: []const u8) bool {
    return std.mem.eql(u8, scalar_type_value, "string") or
        std.mem.eql(u8, scalar_type_value, "boolean") or
        std.mem.eql(u8, scalar_type_value, "datetime") or
        std.mem.eql(u8, scalar_type_value, "integer") or
        std.mem.eql(u8, scalar_type_value, "number");
}

fn isMeasureType(scalar_type_value: []const u8) bool {
    return std.mem.eql(u8, scalar_type_value, "integer") or std.mem.eql(u8, scalar_type_value, "number");
}

fn isTimeType(scalar_type_value: []const u8, format_opt: ?[]const u8) bool {
    if (std.mem.eql(u8, scalar_type_value, "datetime")) return true;
    const format = format_opt orelse return false;
    return std.mem.eql(u8, format, "date") or std.mem.eql(u8, format, "date-time");
}

fn appendPath(alloc: Allocator, prefix: []const u8, child: []const u8) ![]u8 {
    if (prefix.len == 0) return try alloc.dupe(u8, child);
    return try std.fmt.allocPrint(alloc, "{s}.{s}", .{ prefix, child });
}

fn fieldNameFromPath(path: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, path, '.')) |idx| return path[idx + 1 ..];
    return path;
}

test "schema capability plan extracts bounded scalar algebraic fields only" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":4,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"title":{"type":"text"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"},"published":{"type":"boolean"},"meta":{"type":"object","properties":{"region":{"type":"keyword"}}},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 4), plan.schema_version);
    const fingerprint = try capabilityFingerprintAlloc(alloc, plan);
    defer alloc.free(fingerprint);
    try std.testing.expect(fingerprint.len > 0);
    try expectCapability(plan, "doc", "kind", "kind", "string", .group);
    try expectCapability(plan, "doc", "amount", "amount", "number", .group);
    try expectCapability(plan, "doc", "amount", "amount", "number", .measure);
    try expectCapability(plan, "doc", "created_at", "created_at", "datetime", .group);
    try expectCapability(plan, "doc", "created_at", "created_at", "datetime", .time);
    try expectCapability(plan, "doc", "published", "published", "boolean", .group);
    try expectCapability(plan, "doc", "region", "meta.region", "string", .group);
    try std.testing.expect(plan.skipped_complex_fields > 0);
    try std.testing.expectEqual(@as(u32, 0), plan.skipped_unbounded_fields);
}

test "schema capability plan emits non-materializing algebraic config skeleton" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":5,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    const config_json = try configJsonFromPlanAlloc(alloc, "orders", plan);
    defer alloc.free(config_json);

    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try std.testing.expectEqualStrings("orders", parsed_config.value.table);
    try std.testing.expectEqual(@as(u32, 5), parsed_config.value.schema_version);
    try std.testing.expect(parsed_config.value.capability_fingerprint.len > 0);
    try std.testing.expectEqualStrings("current", parsed_config.value.capability_lifecycle_status);
    try std.testing.expectEqual(@as(usize, 3), parsed_config.value.group_fields.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_config.value.measure_fields.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_config.value.time_fields.len);
    try std.testing.expectEqual(@as(usize, 0), parsed_config.value.materializations.len);
}

test "schema capability plan records unbounded dynamic schema metadata" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":7,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"attrs":{"type":"object","additionalProperties":true}},"additionalProperties":true}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    try expectCapability(plan, "doc", "tenant", "tenant", "string", .group);
    try std.testing.expect(plan.skipped_dynamic_fields > 0);
    try std.testing.expect(plan.skipped_unbounded_fields > 0);
}

test "schema capability config can compile directly from schema json" {
    const alloc = std.testing.allocator;
    const config_json = try configJsonFromSchemaJsonAlloc(alloc, "orders",
        \\{"version":6,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"numeric"}},"additionalProperties":false}}}}
    );
    defer alloc.free(config_json);

    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try std.testing.expectEqualStrings("orders", parsed_config.value.table);
    try std.testing.expectEqual(@as(usize, 2), parsed_config.value.group_fields.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_config.value.measure_fields.len);
    try std.testing.expectEqual(@as(usize, 0), parsed_config.value.materializations.len);
}

test "schema capability change classification separates additive from rebuild changes" {
    const alloc = std.testing.allocator;
    var old_schema = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"numeric"}},"additionalProperties":false}}}}
    );
    defer old_schema.deinit(alloc);
    var additive_schema = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":2,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"numeric"},"region":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer additive_schema.deinit(alloc);
    var breaking_schema = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":3,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"amount":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer breaking_schema.deinit(alloc);

    var old_plan = try compilePlanAlloc(alloc, old_schema);
    defer old_plan.deinit(alloc);
    var additive_plan = try compilePlanAlloc(alloc, additive_schema);
    defer additive_plan.deinit(alloc);
    var breaking_plan = try compilePlanAlloc(alloc, breaking_schema);
    defer breaking_plan.deinit(alloc);

    const additive = classifyChange(old_plan, additive_plan);
    try std.testing.expectEqual(@as(u32, 1), additive.old_schema_version);
    try std.testing.expectEqual(@as(u32, 2), additive.new_schema_version);
    try std.testing.expectEqual(@as(u32, 1), additive.added_fields);
    try std.testing.expectEqual(@as(u32, 0), additive.removed_fields);
    try std.testing.expectEqual(@as(u32, 0), additive.changed_type_fields);
    try std.testing.expect(additive.compatible_additive);
    try std.testing.expect(!additive.requires_rebuild);

    const breaking = classifyChange(old_plan, breaking_plan);
    try std.testing.expectEqual(@as(u32, 3), breaking.new_schema_version);
    try std.testing.expectEqual(@as(u32, 0), breaking.added_fields);
    try std.testing.expectEqual(@as(u32, 1), breaking.removed_fields);
    try std.testing.expectEqual(@as(u32, 1), breaking.changed_type_fields);
    try std.testing.expect(!breaking.compatible_additive);
    try std.testing.expect(breaking.requires_rebuild);
}

fn expectCapability(
    plan: Plan,
    document_type: []const u8,
    name: []const u8,
    path: []const u8,
    scalar_type_value: []const u8,
    role: FieldRole,
) !void {
    for (plan.fields) |field| {
        if (field.role != role) continue;
        if (!std.mem.eql(u8, field.document_type, document_type)) continue;
        if (!std.mem.eql(u8, field.name, name)) continue;
        if (!std.mem.eql(u8, field.path, path)) continue;
        if (!std.mem.eql(u8, field.scalar_type, scalar_type_value)) continue;
        return;
    }
    return error.MissingCapability;
}

// ---------------------------------------------------------------------------
// Relational column catalog (see zig/RELATIONAL.md)
//
// The algebraic Plan above projects a schema into group/measure/time *fact*
// roles (a field may appear under several roles). A relational table instead
// needs a flat physical column catalog: exactly one typed column per declared
// property. relationalColumnPlanAlloc compiles a closed TableSchema into that
// catalog. Nested objects, arrays, and `json`-typed fields collapse to a single
// `json` column at their path (stored as bytes, indexed like a document
// subtree) rather than recursing.
// ---------------------------------------------------------------------------

pub const RelationalColumn = struct {
    document_type: []u8,
    name: []u8,
    path: []u8,
    column_type: []u8,
    physical: []u8,
    nullable: bool = true,
    indexed: bool = true,
    is_json: bool = false,

    pub fn deinit(self: *RelationalColumn, alloc: Allocator) void {
        alloc.free(self.document_type);
        alloc.free(self.name);
        alloc.free(self.path);
        alloc.free(self.column_type);
        alloc.free(self.physical);
        self.* = undefined;
    }
};

pub const RelationalPlan = struct {
    schema_version: u32 = 0,
    relational: bool = false,
    columns: []RelationalColumn = &.{},
    skipped_complex_fields: u32 = 0,
    skipped_dynamic_fields: u32 = 0,

    pub fn deinit(self: *RelationalPlan, alloc: Allocator) void {
        for (self.columns) |*column| column.deinit(alloc);
        if (self.columns.len > 0) alloc.free(self.columns);
        self.* = undefined;
    }
};

pub fn relationalColumnPlanAlloc(alloc: Allocator, schema: schema_mod.ParsedTableSchema) !RelationalPlan {
    var columns = std.ArrayListUnmanaged(RelationalColumn).empty;
    errdefer {
        for (columns.items) |*column| column.deinit(alloc);
        columns.deinit(alloc);
    }
    var skipped_complex_fields: u32 = 0;
    var skipped_dynamic_fields: u32 = 0;

    for (schema.document_schemas) |document_schema| {
        if (document_schema.additional_properties_allowed orelse false) skipped_dynamic_fields += 1;
        if (document_schema.additional_properties_schema != null or document_schema.pattern_properties.len > 0 or document_schema.dynamic_infer_types) skipped_dynamic_fields += 1;
        for (document_schema.properties) |property| {
            const required = isRequiredField(document_schema.required_fields, property.name);
            try collectRelationalColumn(
                alloc,
                document_schema.name,
                property,
                required,
                &columns,
                &skipped_complex_fields,
            );
        }
    }

    return .{
        .schema_version = schema.version,
        .relational = schema.storage_mode == .relational,
        .columns = try columns.toOwnedSlice(alloc),
        .skipped_complex_fields = skipped_complex_fields,
        .skipped_dynamic_fields = skipped_dynamic_fields,
    };
}

pub fn relationalColumnsJsonAlloc(alloc: Allocator, table_name: []const u8, plan: RelationalPlan) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, "table");
    try out.append(alloc, ':');
    try appendJsonString(alloc, &out, table_name);
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "schema_version");
    try appendFmt(alloc, &out, ":{d}", .{plan.schema_version});
    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "relational");
    try out.appendSlice(alloc, if (plan.relational) ":true," else ":false,");
    try appendJsonString(alloc, &out, "columns");
    try out.appendSlice(alloc, ":[");
    for (plan.columns, 0..) |column, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, &out, "document_type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, &out, column.document_type);
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, &out, column.name);
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "path");
        try out.append(alloc, ':');
        try appendJsonString(alloc, &out, column.path);
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, &out, column.column_type);
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "physical");
        try out.append(alloc, ':');
        try appendJsonString(alloc, &out, column.physical);
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "nullable");
        try out.appendSlice(alloc, if (column.nullable) ":true," else ":false,");
        try appendJsonString(alloc, &out, "indexed");
        try out.appendSlice(alloc, if (column.indexed) ":true," else ":false,");
        try appendJsonString(alloc, &out, "is_json");
        try out.appendSlice(alloc, if (column.is_json) ":true" else ":false");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");

    return try out.toOwnedSlice(alloc);
}

fn collectRelationalColumn(
    alloc: Allocator,
    document_type: []const u8,
    property: anytype,
    required: bool,
    columns: *std.ArrayListUnmanaged(RelationalColumn),
    skipped_complex_fields: *u32,
) !void {
    const column_type = relationalColumnType(property) orelse {
        skipped_complex_fields.* += 1;
        return;
    };
    const indexed = if (property.antfly_index) |value| value else true;
    const is_json = std.mem.eql(u8, column_type, "json");
    try columns.append(alloc, .{
        .document_type = try alloc.dupe(u8, document_type),
        .name = try alloc.dupe(u8, property.name),
        .path = try alloc.dupe(u8, property.name),
        .column_type = try alloc.dupe(u8, column_type),
        .physical = try alloc.dupe(u8, physicalForColumnType(column_type)),
        .nullable = !required,
        .indexed = indexed,
        .is_json = is_json,
    });
}

fn relationalColumnType(property: anytype) ?[]const u8 {
    if (property.field_type) |field_type| {
        if (std.mem.eql(u8, field_type, "keyword") or
            std.mem.eql(u8, field_type, "link") or
            std.mem.eql(u8, field_type, "string") or
            std.mem.eql(u8, field_type, "text") or
            std.mem.eql(u8, field_type, "html") or
            std.mem.eql(u8, field_type, "search_as_you_type")) return "string";
        if (std.mem.eql(u8, field_type, "blob")) return "blob";
        if (std.mem.eql(u8, field_type, "boolean")) return "boolean";
        if (std.mem.eql(u8, field_type, "datetime")) return "datetime";
        if (std.mem.eql(u8, field_type, "integer")) return "integer";
        if (std.mem.eql(u8, field_type, "numeric") or std.mem.eql(u8, field_type, "number")) return "number";
        if (std.mem.eql(u8, field_type, "geopoint")) return "geopoint";
        if (std.mem.eql(u8, field_type, "geoshape")) return "geoshape";
        if (std.mem.eql(u8, field_type, "json") or
            std.mem.eql(u8, field_type, "object") or
            std.mem.eql(u8, field_type, "array")) return "json";
        if (property.integer_only) return "integer";
        return null;
    }
    if (property.integer_only) return "integer";
    if (property.properties.len > 0 or
        property.item != null or
        (property.additional_properties_allowed orelse false) or
        property.additional_properties_schema != null or
        property.pattern_properties.len > 0 or
        property.dynamic_infer_types) return "json";
    if (property.const_value != null or property.enum_values.len > 0) return "string";
    return null;
}

fn physicalForColumnType(column_type: []const u8) []const u8 {
    // Datetimes match the engine's timestamp doc values (raw u64 epoch ns,
    // read via getU64). Integers and numbers both map to f64, matching how the
    // introducer detects numeric fields and how search/query range readers
    // consume them (getF64 / readF64Chunk) -- this is what lets relational
    // typed columns reuse the existing predicate readers.
    if (std.mem.eql(u8, column_type, "datetime")) return "u64_val";
    if (std.mem.eql(u8, column_type, "integer") or std.mem.eql(u8, column_type, "number")) return "f64_val";
    if (std.mem.eql(u8, column_type, "boolean")) return "bool_val";
    if (std.mem.eql(u8, column_type, "geopoint")) return "geo_point";
    return "bytes_val";
}

fn isRequiredField(required_fields: []const []const u8, name: []const u8) bool {
    for (required_fields) |field_name| {
        if (std.mem.eql(u8, field_name, name)) return true;
    }
    return false;
}

test "relational column plan emits one typed column per declared property" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"datetime"},"active":{"type":"boolean"},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"tags":{"type":"array","items":{"type":"keyword"}},"payload":{"type":"json"}},"required":["id","amount"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.relational);
    try std.testing.expectEqual(@as(u32, 4), plan.schema_version);
    try std.testing.expectEqual(@as(usize, 7), plan.columns.len);
    try std.testing.expectEqual(@as(u32, 0), plan.skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 0), plan.skipped_dynamic_fields);

    try expectRelationalColumn(plan, "row", "id", "string", "bytes_val", false, false);
    try expectRelationalColumn(plan, "row", "amount", "number", "f64_val", false, false);
    try expectRelationalColumn(plan, "row", "created_at", "datetime", "u64_val", true, false);
    try expectRelationalColumn(plan, "row", "active", "boolean", "bool_val", true, false);
    try expectRelationalColumn(plan, "row", "attrs", "json", "bytes_val", true, true);
    try expectRelationalColumn(plan, "row", "tags", "json", "bytes_val", true, true);
    try expectRelationalColumn(plan, "row", "payload", "json", "bytes_val", true, true);
}

test "relational column plan defaults to document storage mode" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    try std.testing.expect(!plan.relational);
    try std.testing.expectEqual(@as(usize, 1), plan.columns.len);
    try expectRelationalColumn(plan, "doc", "id", "string", "bytes_val", true, false);
}

test "relational column plan serializes a column catalog" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const catalog_json = try relationalColumnsJsonAlloc(alloc, "rows", plan);
    defer alloc.free(catalog_json);

    var parsed_catalog = try std.json.parseFromSlice(std.json.Value, alloc, catalog_json, .{});
    defer parsed_catalog.deinit();
    const root = parsed_catalog.value.object;
    try std.testing.expectEqualStrings("rows", root.get("table").?.string);
    try std.testing.expectEqual(@as(i64, 9), root.get("schema_version").?.integer);
    try std.testing.expect(root.get("relational").?.bool);
    try std.testing.expectEqual(@as(usize, 2), root.get("columns").?.array.items.len);
}

fn expectRelationalColumn(
    plan: RelationalPlan,
    document_type: []const u8,
    name: []const u8,
    column_type: []const u8,
    physical: []const u8,
    nullable: bool,
    is_json: bool,
) !void {
    for (plan.columns) |column| {
        if (!std.mem.eql(u8, column.document_type, document_type)) continue;
        if (!std.mem.eql(u8, column.name, name)) continue;
        if (!std.mem.eql(u8, column.column_type, column_type)) continue;
        if (!std.mem.eql(u8, column.physical, physical)) continue;
        if (column.nullable != nullable) continue;
        if (column.is_json != is_json) continue;
        return;
    }
    return error.MissingColumn;
}

// ---------------------------------------------------------------------------
// Relational write-path projection (Phase 2, see zig/RELATIONAL.md)
//
// projectRelationalRowAlloc turns a document into one typed cell per declared
// column, ready to hand to section/typed_doc_values.zig at segment-build time:
//   - missing/null value on a non-nullable column -> error.MissingRequiredColumn
//   - value that does not match the declared column type -> error.InvalidColumnValue
//   - json columns are stringified to bytes (and flagged is_json so the caller
//     can additionally index the subtree like a document)
//
// Numeric physical encoding matches the engine's existing doc values so the
// columns are consumable by the existing search/query range readers:
//   - number / integer -> f64 (native), like detectTypedValue + getF64
//   - datetime -> raw u64 epoch ns (like the timestamp doc values read via
//                 getU64; RFC3339 string parsing is a follow-up, epoch
//                 integers / integer-strings are accepted today)
//   - boolean  -> bool, geopoint -> packed lat/lon, string/blob/geoshape -> bytes
// ---------------------------------------------------------------------------

const typed_doc_values = @import("../../../section/typed_doc_values.zig");

pub const PhysicalType = enum { u64_val, f64_val, bytes_val, bool_val, geo_point };

pub const GeoPoint = struct { lat: f64, lon: f64 };

pub const ColumnValue = union(PhysicalType) {
    u64_val: u64,
    f64_val: f64,
    bytes_val: []const u8,
    bool_val: bool,
    geo_point: GeoPoint,
};

pub const RelationalCell = struct {
    column: usize,
    present: bool = false,
    is_json: bool = false,
    value: ColumnValue = .{ .bool_val = false },
};

pub const RelationalRow = struct {
    cells: []RelationalCell = &.{},
    bytes_pool: [][]u8 = &.{},

    pub fn deinit(self: *RelationalRow, alloc: Allocator) void {
        for (self.bytes_pool) |buffer| alloc.free(buffer);
        if (self.bytes_pool.len > 0) alloc.free(self.bytes_pool);
        if (self.cells.len > 0) alloc.free(self.cells);
        self.* = undefined;
    }

    pub fn cell(self: RelationalRow, column_index: usize) ?RelationalCell {
        for (self.cells) |candidate| {
            if (candidate.column == column_index and candidate.present) return candidate;
        }
        return null;
    }
};

pub fn typedValue(value: ColumnValue) typed_doc_values.TypedValue {
    return switch (value) {
        .u64_val => |v| .{ .u64_val = v },
        .f64_val => |v| .{ .f64_val = v },
        .bytes_val => |v| .{ .bytes_val = v },
        .bool_val => |v| .{ .bool_val = v },
        .geo_point => |v| .{ .geo_point = .{ .lat = v.lat, .lon = v.lon } },
    };
}

pub fn projectRelationalRowAlloc(alloc: Allocator, plan: RelationalPlan, root: std.json.Value) !RelationalRow {
    if (root != .object) return error.NotAnObject;

    const cells = try alloc.alloc(RelationalCell, plan.columns.len);
    errdefer alloc.free(cells);
    var pool = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (pool.items) |buffer| alloc.free(buffer);
        pool.deinit(alloc);
    }

    for (plan.columns, 0..) |column, i| {
        cells[i] = .{ .column = i, .present = false, .is_json = column.is_json };
        const found = valueAtJsonPath(root, column.path);
        if (found == null or found.? == .null) {
            if (!column.nullable) return error.MissingRequiredColumn;
            continue;
        }
        const coerced = (try coerceColumnValue(alloc, column.column_type, found.?)) orelse return error.InvalidColumnValue;
        if (coerced.owned) |buffer| try pool.append(alloc, buffer);
        cells[i] = .{ .column = i, .present = true, .is_json = column.is_json, .value = coerced.value };
    }

    return .{ .cells = cells, .bytes_pool = try pool.toOwnedSlice(alloc) };
}

const Coerced = struct { value: ColumnValue, owned: ?[]u8 = null };

fn coerceColumnValue(alloc: Allocator, column_type: []const u8, json_value: std.json.Value) !?Coerced {
    if (std.mem.eql(u8, column_type, "json")) {
        const bytes = try stringifyJsonValueAlloc(alloc, json_value);
        return Coerced{ .value = .{ .bytes_val = bytes }, .owned = bytes };
    }
    if (std.mem.eql(u8, column_type, "string") or
        std.mem.eql(u8, column_type, "blob") or
        std.mem.eql(u8, column_type, "geoshape"))
    {
        switch (json_value) {
            .string => |text| {
                const bytes = try alloc.dupe(u8, text);
                return Coerced{ .value = .{ .bytes_val = bytes }, .owned = bytes };
            },
            else => return null,
        }
    }
    if (std.mem.eql(u8, column_type, "boolean")) {
        switch (json_value) {
            .bool => |flag| return Coerced{ .value = .{ .bool_val = flag } },
            else => return null,
        }
    }
    if (std.mem.eql(u8, column_type, "number") or std.mem.eql(u8, column_type, "integer")) {
        switch (json_value) {
            .float => |number| return Coerced{ .value = .{ .f64_val = number } },
            .integer => |number| return Coerced{ .value = .{ .f64_val = @floatFromInt(number) } },
            .string => |text| {
                if (std.mem.eql(u8, column_type, "integer")) {
                    const parsed = std.fmt.parseInt(i64, text, 10) catch return null;
                    return Coerced{ .value = .{ .f64_val = @floatFromInt(parsed) } };
                }
                return null;
            },
            else => return null,
        }
    }
    if (std.mem.eql(u8, column_type, "datetime")) {
        const number = integerFromJson(json_value) orelse return null;
        return Coerced{ .value = .{ .u64_val = @bitCast(number) } };
    }
    if (std.mem.eql(u8, column_type, "geopoint")) {
        const point = geoPointFromJson(json_value) orelse return null;
        return Coerced{ .value = .{ .geo_point = point } };
    }
    return null;
}

fn integerFromJson(json_value: std.json.Value) ?i64 {
    switch (json_value) {
        .integer => |number| return number,
        .string => |text| return std.fmt.parseInt(i64, text, 10) catch null,
        else => return null,
    }
}

fn geoPointFromJson(json_value: std.json.Value) ?GeoPoint {
    switch (json_value) {
        .object => |object| {
            const lat = jsonNumber(object.get("lat") orelse return null) orelse return null;
            const lon = jsonNumber(object.get("lon") orelse return null) orelse return null;
            return GeoPoint{ .lat = lat, .lon = lon };
        },
        else => return null,
    }
}

fn jsonNumber(json_value: std.json.Value) ?f64 {
    switch (json_value) {
        .float => |number| return number,
        .integer => |number| return @floatFromInt(number),
        else => return null,
    }
}

fn stringifyJsonValueAlloc(alloc: Allocator, json_value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(json_value, .{})});
}

// ---------------------------------------------------------------------------
// Document reconstruction (Phase 5 foundation, see zig/RELATIONAL.md)
//
// reconstructRelationalDocumentAlloc rebuilds a JSON document from a projected
// RelationalRow. This proves the typed columns carry enough information to
// reconstruct the document, which is the prerequisite for making columns the
// authoritative store (dropping the JSON blob for non-json columns). It does
// not yet flip the storage switch -- the read path still uses the stored blob.
//
// Emitted by column type:
//   string/blob/geoshape -> JSON string      (bytes preserved verbatim)
//   numeric/integer       -> JSON number      (from f64)
//   datetime              -> JSON number      (epoch ns from u64)
//   boolean               -> JSON true/false
//   geopoint              -> {"lat":..,"lon":..}
//   json                  -> the stored subtree bytes, embedded verbatim
// Absent (nullable, omitted) columns are skipped, matching how they were
// projected. Columns are emitted by their declared path (top-level today).
// ---------------------------------------------------------------------------

pub fn reconstructRelationalDocumentAlloc(alloc: Allocator, plan: RelationalPlan, row: RelationalRow) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.append(alloc, '{');
    var emitted: usize = 0;
    for (plan.columns, 0..) |column, i| {
        const candidate = row.cell(i) orelse continue;
        if (emitted > 0) try out.append(alloc, ',');
        emitted += 1;
        try appendJsonString(alloc, &out, column.path);
        try out.append(alloc, ':');
        if (column.is_json) {
            // Stored bytes are already valid JSON; embed verbatim.
            try out.appendSlice(alloc, candidate.value.bytes_val);
        } else {
            try appendReconstructedScalar(alloc, &out, column.column_type, candidate.value);
        }
    }
    try out.append(alloc, '}');

    return try out.toOwnedSlice(alloc);
}

fn appendReconstructedScalar(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    column_type: []const u8,
    value: ColumnValue,
) !void {
    if (std.mem.eql(u8, column_type, "string") or
        std.mem.eql(u8, column_type, "blob") or
        std.mem.eql(u8, column_type, "geoshape"))
    {
        try appendJsonString(alloc, out, value.bytes_val);
        return;
    }
    if (std.mem.eql(u8, column_type, "boolean")) {
        try out.appendSlice(alloc, if (value.bool_val) "true" else "false");
        return;
    }
    if (std.mem.eql(u8, column_type, "number") or std.mem.eql(u8, column_type, "integer")) {
        try appendFmt(alloc, out, "{d}", .{value.f64_val});
        return;
    }
    if (std.mem.eql(u8, column_type, "datetime")) {
        try appendFmt(alloc, out, "{d}", .{value.u64_val});
        return;
    }
    if (std.mem.eql(u8, column_type, "geopoint")) {
        try appendFmt(alloc, out, "{{\"lat\":{d},\"lon\":{d}}}", .{ value.geo_point.lat, value.geo_point.lon });
        return;
    }
    // Unknown scalar kind: emit JSON null rather than corrupt the document.
    try out.appendSlice(alloc, "null");
}

fn valueAtJsonPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var it = std.mem.splitScalar(u8, path, '.');
    while (it.next()) |segment| {
        switch (current) {
            .object => |object| current = object.get(segment) orelse return null,
            else => return null,
        }
    }
    return current;
}

fn relationalTestPlanAlloc(alloc: Allocator) !RelationalPlan {
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"qty":{"type":"integer"},"ts":{"type":"datetime"},"active":{"type":"boolean"},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"payload":{"type":"json"}},"required":["id","amount"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    return try relationalColumnPlanAlloc(alloc, parsed);
}

fn relationalColumnIndex(plan: RelationalPlan, name: []const u8) ?usize {
    for (plan.columns, 0..) |column, i| {
        if (std.mem.eql(u8, column.name, name)) return i;
    }
    return null;
}

test "relational document reconstructs from typed cells round-trip" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"qty":7,"ts":1000,"active":true,"attrs":{"k":"v"},"payload":[1,2,3]}
    , .{});
    defer doc.deinit();

    var row = try projectRelationalRowAlloc(alloc, plan, doc.value);
    defer row.deinit(alloc);

    const rebuilt_json = try reconstructRelationalDocumentAlloc(alloc, plan, row);
    defer alloc.free(rebuilt_json);

    var rebuilt = try std.json.parseFromSlice(std.json.Value, alloc, rebuilt_json, .{});
    defer rebuilt.deinit();
    const obj = rebuilt.value.object;

    try std.testing.expectEqualStrings("abc", obj.get("id").?.string);
    try std.testing.expectEqual(@as(f64, 12.5), jsonNumberOf(obj.get("amount").?));
    // integer column reconstructs as a JSON number (7 parses as integer)
    try std.testing.expectEqual(@as(i64, 7), obj.get("qty").?.integer);
    try std.testing.expectEqual(@as(i64, 1000), obj.get("ts").?.integer);
    try std.testing.expect(obj.get("active").?.bool);
    // json columns reconstruct as their original subtrees
    try std.testing.expectEqualStrings("v", obj.get("attrs").?.object.get("k").?.string);
    try std.testing.expectEqual(@as(usize, 3), obj.get("payload").?.array.items.len);
}

test "relational reconstruction omits absent nullable columns" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    // Only the required columns are present; nullable columns are omitted.
    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"x","amount":0.0}
    , .{});
    defer doc.deinit();

    var row = try projectRelationalRowAlloc(alloc, plan, doc.value);
    defer row.deinit(alloc);

    const rebuilt_json = try reconstructRelationalDocumentAlloc(alloc, plan, row);
    defer alloc.free(rebuilt_json);

    var rebuilt = try std.json.parseFromSlice(std.json.Value, alloc, rebuilt_json, .{});
    defer rebuilt.deinit();
    const obj = rebuilt.value.object;

    try std.testing.expectEqual(@as(usize, 2), obj.count());
    try std.testing.expectEqualStrings("x", obj.get("id").?.string);
    try std.testing.expectEqual(@as(f64, 0.0), jsonNumberOf(obj.get("amount").?));
    try std.testing.expect(obj.get("qty") == null);
    try std.testing.expect(obj.get("payload") == null);
}

/// Read a JSON number regardless of whether it parsed as integer or float
/// (reconstruction formats f64 0.0 as "0", which re-parses as .integer).
fn jsonNumberOf(value: std.json.Value) f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => std.math.nan(f64),
    };
}

test "relational projection enforces required columns and types" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    // Missing a required column.
    var missing = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a"}
    , .{});
    defer missing.deinit();
    try std.testing.expectError(error.MissingRequiredColumn, projectRelationalRowAlloc(alloc, plan, missing.value));

    // Required column present but wrong type (string column given a number).
    var wrong = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":5,"amount":1.0}
    , .{});
    defer wrong.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, wrong.value));
}

test "relational projection yields typed cells" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"qty":7,"ts":1000,"active":true,"attrs":{"k":"v"},"payload":[1,2,3]}
    , .{});
    defer doc.deinit();

    var row = try projectRelationalRowAlloc(alloc, plan, doc.value);
    defer row.deinit(alloc);

    const id = row.cell(relationalColumnIndex(plan, "id").?).?;
    try std.testing.expectEqualStrings("abc", id.value.bytes_val);
    const amount = row.cell(relationalColumnIndex(plan, "amount").?).?;
    try std.testing.expectEqual(@as(f64, 12.5), amount.value.f64_val);
    const qty = row.cell(relationalColumnIndex(plan, "qty").?).?;
    try std.testing.expectEqual(@as(f64, 7), qty.value.f64_val);
    const ts = row.cell(relationalColumnIndex(plan, "ts").?).?;
    try std.testing.expectEqual(@as(u64, 1000), ts.value.u64_val);
    const active = row.cell(relationalColumnIndex(plan, "active").?).?;
    try std.testing.expect(active.value.bool_val);

    // A nullable column omitted from the document yields no cell.
    var sparse = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"x","amount":0.0}
    , .{});
    defer sparse.deinit();
    var sparse_row = try projectRelationalRowAlloc(alloc, plan, sparse.value);
    defer sparse_row.deinit(alloc);
    try std.testing.expect(sparse_row.cell(relationalColumnIndex(plan, "qty").?) == null);

    // json columns are stringified and flagged.
    const attrs = row.cell(relationalColumnIndex(plan, "attrs").?).?;
    try std.testing.expect(attrs.is_json);
    var attrs_parsed = try std.json.parseFromSlice(std.json.Value, alloc, attrs.value.bytes_val, .{});
    defer attrs_parsed.deinit();
    try std.testing.expectEqualStrings("v", attrs_parsed.value.object.get("k").?.string);
    const payload = row.cell(relationalColumnIndex(plan, "payload").?).?;
    try std.testing.expect(payload.is_json);
    var payload_parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload.value.bytes_val, .{});
    defer payload_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), payload_parsed.value.array.items.len);
}

test "relational cells round-trip through typed_doc_values storage" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":2.5,"qty":42,"ts":1000,"active":true,"payload":1}
    , .{});
    defer doc.deinit();

    var row = try projectRelationalRowAlloc(alloc, plan, doc.value);
    defer row.deinit(alloc);

    // Drive the real typed_doc_values writer/reader for each typed-scan column:
    // datetime -> u64, number -> f64, boolean -> bool.
    try expectTypedColumnRoundTrip(alloc, .u64_val, typedValue(row.cell(relationalColumnIndex(plan, "ts").?).?.value));
    try expectTypedColumnRoundTrip(alloc, .f64_val, typedValue(row.cell(relationalColumnIndex(plan, "amount").?).?.value));
    try expectTypedColumnRoundTrip(alloc, .bool_val, typedValue(row.cell(relationalColumnIndex(plan, "active").?).?.value));
}

fn expectTypedColumnRoundTrip(
    alloc: Allocator,
    value_type: typed_doc_values.ValueType,
    value: typed_doc_values.TypedValue,
) !void {
    var writer = typed_doc_values.TypedDocValuesWriter.init(alloc, value_type, typed_doc_values.default_chunk_size);
    defer writer.deinit();
    try writer.add(0, value);
    const bytes = try writer.build();
    defer alloc.free(bytes);

    const reader = try typed_doc_values.TypedDocValuesReader.init(alloc, bytes);
    switch (value_type) {
        .u64_val => try std.testing.expectEqual(value.u64_val, (try reader.getU64(0)).?),
        .f64_val => try std.testing.expectEqual(value.f64_val, (try reader.getF64(0)).?),
        .bool_val => try std.testing.expectEqual(value.bool_val, (try reader.getBool(0)).?),
        else => unreachable,
    }
}

// ---------------------------------------------------------------------------
// Introducer hand-off (Phase 3, see zig/RELATIONAL.md)
//
// The segment builder (introducer.zig) accepts caller-supplied typed columns
// via TextDocument.typed_fields, which bypasses value-based type detection.
// relationalTypedColumnsAlloc produces exactly that input from a relational
// document: one typed field per present, non-json declared column, carrying the
// schema-declared physical type. Because json columns are skipped, a json
// subtree is never exploded into typed columns (it is indexed as a document
// subtree instead), and because the types come from the schema rather than from
// detection, they are authoritative.
//
// The fields use typed_doc_values types directly; the orchestration layer maps
// each RelationalTypedField to an introducer.TypedFieldValue by renaming
// `name` -> `field_name` (the other two fields are identical), keeping this
// module independent of the introducer/segment layer.
// ---------------------------------------------------------------------------

pub const RelationalTypedField = struct {
    name: []const u8,
    value_type: typed_doc_values.ValueType,
    value: typed_doc_values.TypedValue,
};

pub const RelationalTypedColumns = struct {
    row: RelationalRow,
    fields: []RelationalTypedField = &.{},

    pub fn deinit(self: *RelationalTypedColumns, alloc: Allocator) void {
        if (self.fields.len > 0) alloc.free(self.fields);
        self.row.deinit(alloc);
        self.* = undefined;
    }
};

pub fn relationalTypedColumnsAlloc(alloc: Allocator, plan: RelationalPlan, root: std.json.Value) !RelationalTypedColumns {
    var row = try projectRelationalRowAlloc(alloc, plan, root);
    errdefer row.deinit(alloc);

    var fields = std.ArrayListUnmanaged(RelationalTypedField).empty;
    errdefer fields.deinit(alloc);

    for (row.cells) |candidate| {
        if (!candidate.present) continue;
        const column = plan.columns[candidate.column];
        const value_type = typedDocValueTypeForColumnType(column.column_type) orelse continue;
        try fields.append(alloc, .{
            .name = column.name,
            .value_type = value_type,
            .value = typedValue(candidate.value),
        });
    }

    return .{ .row = row, .fields = try fields.toOwnedSlice(alloc) };
}

/// Phase 5 (authoritative columns): project the *full reconstructable* column
/// set. Unlike relationalTypedColumnsAlloc (which emits only the columns routed
/// to typed-doc-value predicate scans), this emits every present column as a
/// typed-doc-value field, including string/blob/geoshape and json columns as
/// `bytes_val`. Persisting this set lets the document be rebuilt from columns
/// alone (via reconstructRelationalDocumentAlloc) -- the prerequisite for
/// dropping the JSON blob. Field order matches the plan's column order.
pub fn relationalStorageColumnsAlloc(alloc: Allocator, plan: RelationalPlan, root: std.json.Value) !RelationalTypedColumns {
    var row = try projectRelationalRowAlloc(alloc, plan, root);
    errdefer row.deinit(alloc);

    var fields = std.ArrayListUnmanaged(RelationalTypedField).empty;
    errdefer fields.deinit(alloc);

    for (plan.columns, 0..) |column, i| {
        const candidate = row.cell(i) orelse continue;
        try fields.append(alloc, .{
            .name = column.name,
            .value_type = storageValueTypeForColumnType(column.column_type),
            .value = typedValue(candidate.value),
        });
    }

    return .{ .row = row, .fields = try fields.toOwnedSlice(alloc) };
}

/// The typed_doc_values type used to *persist* a column for reconstruction.
/// Same numeric/datetime/boolean/geopoint mapping as the scan path, but
/// everything else (string/blob/geoshape/json) is stored as `bytes_val`.
fn storageValueTypeForColumnType(column_type: []const u8) typed_doc_values.ValueType {
    return typedDocValueTypeForColumnType(column_type) orelse .bytes_val;
}

/// The typed_doc_values type for a column, or null when the column is not stored
/// as a typed doc value. Only numeric/datetime/boolean/geopoint columns become
/// typed columns; keyword/text columns use the full-text/inverted index for
/// term and range predicates, and json columns are indexed as document
/// subtrees -- so those return null and are not emitted as typed fields.
fn typedDocValueTypeForColumnType(column_type: []const u8) ?typed_doc_values.ValueType {
    if (std.mem.eql(u8, column_type, "datetime")) return .u64_val;
    if (std.mem.eql(u8, column_type, "integer") or std.mem.eql(u8, column_type, "number")) return .f64_val;
    if (std.mem.eql(u8, column_type, "boolean")) return .bool_val;
    if (std.mem.eql(u8, column_type, "geopoint")) return .geo_point;
    return null;
}

fn relationalFieldByName(fields: []const RelationalTypedField, name: []const u8) ?RelationalTypedField {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return field;
    }
    return null;
}

test "relational typed columns exclude json and carry declared physical types" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"qty":7,"ts":1000,"active":true,"attrs":{"k":"v"},"payload":[1,2,3]}
    , .{});
    defer doc.deinit();

    var columns = try relationalTypedColumnsAlloc(alloc, plan, doc.value);
    defer columns.deinit(alloc);

    // Only numeric/datetime/boolean columns become typed doc values. The
    // keyword column (id) goes to the full-text index, and json columns (attrs,
    // payload) are indexed as subtrees -- none are emitted as typed fields.
    try std.testing.expect(relationalFieldByName(columns.fields, "id") == null);
    try std.testing.expect(relationalFieldByName(columns.fields, "attrs") == null);
    try std.testing.expect(relationalFieldByName(columns.fields, "payload") == null);
    try std.testing.expectEqual(@as(usize, 4), columns.fields.len);

    try std.testing.expectEqual(typed_doc_values.ValueType.f64_val, relationalFieldByName(columns.fields, "amount").?.value_type);
    try std.testing.expectEqual(typed_doc_values.ValueType.f64_val, relationalFieldByName(columns.fields, "qty").?.value_type);
    try std.testing.expectEqual(typed_doc_values.ValueType.u64_val, relationalFieldByName(columns.fields, "ts").?.value_type);
    try std.testing.expectEqual(typed_doc_values.ValueType.bool_val, relationalFieldByName(columns.fields, "active").?.value_type);
    try std.testing.expectEqual(@as(f64, 12.5), relationalFieldByName(columns.fields, "amount").?.value.f64_val);
}

test "relational storage columns persist and reconstruct the document" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);

    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"qty":7,"ts":1000,"active":true,"attrs":{"k":"v"},"payload":[1,2,3]}
    , .{});
    defer doc.deinit();

    // Storage projection emits ALL present columns (strings + json as bytes).
    var columns = try relationalStorageColumnsAlloc(alloc, plan, doc.value);
    defer columns.deinit(alloc);
    // All 7 declared columns are present in this document.
    try std.testing.expectEqual(@as(usize, 7), columns.fields.len);
    try std.testing.expectEqual(typed_doc_values.ValueType.bytes_val, relationalFieldByName(columns.fields, "id").?.value_type);
    try std.testing.expectEqual(typed_doc_values.ValueType.bytes_val, relationalFieldByName(columns.fields, "payload").?.value_type);

    // Persist each field through the real typed_doc_values writer at doc_id 0,
    // then read every value back from its own section -- proving the columns
    // survive a full serialize/deserialize round-trip.
    var rebuilt_cells = std.ArrayListUnmanaged(RelationalCell).empty;
    defer rebuilt_cells.deinit(alloc);
    var byte_bufs = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (byte_bufs.items) |b| alloc.free(b);
        byte_bufs.deinit(alloc);
    }

    for (columns.fields) |field| {
        const column_index = relationalColumnIndex(plan, field.name).?;
        var writer = typed_doc_values.TypedDocValuesWriter.init(alloc, field.value_type, typed_doc_values.default_chunk_size);
        defer writer.deinit();
        try writer.add(0, field.value);
        const section = try writer.build();
        defer alloc.free(section);

        const reader = try typed_doc_values.TypedDocValuesReader.init(alloc, section);
        const value: ColumnValue = switch (field.value_type) {
            .u64_val => .{ .u64_val = (try reader.getU64(0)).? },
            .f64_val => .{ .f64_val = (try reader.getF64(0)).? },
            .bool_val => .{ .bool_val = (try reader.getBool(0)).? },
            .geo_point => blk: {
                const gp = (try reader.getGeoPoint(0)).?;
                break :blk .{ .geo_point = .{ .lat = gp.lat, .lon = gp.lon } };
            },
            .bytes_val => blk: {
                const bytes = (try reader.getBytes(0)).?;
                try byte_bufs.append(alloc, bytes);
                break :blk .{ .bytes_val = bytes };
            },
        };
        try rebuilt_cells.append(alloc, .{
            .column = column_index,
            .present = true,
            .is_json = plan.columns[column_index].is_json,
            .value = value,
        });
    }

    const rebuilt_row = RelationalRow{ .cells = rebuilt_cells.items, .bytes_pool = &.{} };
    const rebuilt_json = try reconstructRelationalDocumentAlloc(alloc, plan, rebuilt_row);
    defer alloc.free(rebuilt_json);

    var rebuilt = try std.json.parseFromSlice(std.json.Value, alloc, rebuilt_json, .{});
    defer rebuilt.deinit();
    const obj = rebuilt.value.object;

    try std.testing.expectEqualStrings("abc", obj.get("id").?.string);
    try std.testing.expectEqual(@as(f64, 12.5), jsonNumberOf(obj.get("amount").?));
    try std.testing.expectEqual(@as(i64, 7), obj.get("qty").?.integer);
    try std.testing.expectEqual(@as(i64, 1000), obj.get("ts").?.integer);
    try std.testing.expect(obj.get("active").?.bool);
    try std.testing.expectEqualStrings("v", obj.get("attrs").?.object.get("k").?.string);
    try std.testing.expectEqual(@as(usize, 3), obj.get("payload").?.array.items.len);
}
