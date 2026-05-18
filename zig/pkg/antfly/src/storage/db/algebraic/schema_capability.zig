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
