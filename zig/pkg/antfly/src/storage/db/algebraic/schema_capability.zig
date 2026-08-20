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
const relational_row_codec = @import("relational_row_codec.zig");
const schema_mod = @import("../../../schema/mod.zig");
const geo_mod = @import("../../../search/geo.zig");

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

/// A table-level dynamic template compiled into a runtime-adaptive algebraic
/// rule. Only templates that resolve to a bounded scalar type become rules; the
/// selectors are carried verbatim and evaluated per-document at ingest time.
pub const DynamicRuleCapability = struct {
    name: []u8,
    match: ?[]u8 = null,
    unmatch: ?[]u8 = null,
    path_match: ?[]u8 = null,
    path_unmatch: ?[]u8 = null,
    match_mapping_type: ?[]u8 = null,
    scalar_type: []u8,

    pub fn deinit(self: *DynamicRuleCapability, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.match) |v| alloc.free(v);
        if (self.unmatch) |v| alloc.free(v);
        if (self.path_match) |v| alloc.free(v);
        if (self.path_unmatch) |v| alloc.free(v);
        if (self.match_mapping_type) |v| alloc.free(v);
        alloc.free(self.scalar_type);
        self.* = undefined;
    }
};

pub const Plan = struct {
    schema_version: u32 = 0,
    fields: []FieldCapability = &.{},
    dynamic_rules: []DynamicRuleCapability = &.{},
    skipped_dynamic_fields: u32 = 0,
    skipped_complex_fields: u32 = 0,
    skipped_unbounded_fields: u32 = 0,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.fields) |*field| field.deinit(alloc);
        if (self.fields.len > 0) alloc.free(self.fields);
        for (self.dynamic_rules) |*rule| rule.deinit(alloc);
        if (self.dynamic_rules.len > 0) alloc.free(self.dynamic_rules);
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
    var dynamic_rules = std.ArrayListUnmanaged(DynamicRuleCapability).empty;
    errdefer {
        for (dynamic_rules.items) |*rule| rule.deinit(alloc);
        dynamic_rules.deinit(alloc);
    }
    var skipped_dynamic_fields: u32 = 0;
    var skipped_complex_fields: u32 = 0;
    var skipped_unbounded_fields: u32 = 0;

    for (schema.dynamic_templates) |tmpl| {
        // An algebraic rule needs a name/path selector (`match` / `path_match`).
        // `match_mapping_type` alone is NOT enough: it can be evaluated at ingest
        // (a value is present) but never at query time (no value), so a
        // mapping-type-only rule would project docfacts that no query can ever
        // resolve. Such templates stay on the schemaless path-fact path so ingest
        // and query resolution remain symmetric.
        const has_named_selector = tmpl.match_pattern != null or tmpl.path_match != null;
        const scalar = boundedScalarForTemplateType(tmpl.field_type orelse "text");
        if (!has_named_selector or scalar == null) {
            // Unbounded/open text templates and selector-less / mapping-type-only
            // templates stay on the schemaless path-fact path rather than typed
            // docfacts.
            skipped_dynamic_fields += 1;
            skipped_unbounded_fields += 1;
            continue;
        }
        const name = try alloc.dupe(u8, tmpl.name);
        errdefer alloc.free(name);
        const match = try dupeOptional(alloc, tmpl.match_pattern);
        errdefer if (match) |value| alloc.free(value);
        const unmatch = try dupeOptional(alloc, tmpl.unmatch_pattern);
        errdefer if (unmatch) |value| alloc.free(value);
        const path_match = try dupeOptional(alloc, tmpl.path_match);
        errdefer if (path_match) |value| alloc.free(value);
        const path_unmatch = try dupeOptional(alloc, tmpl.path_unmatch);
        errdefer if (path_unmatch) |value| alloc.free(value);
        const match_mapping_type = try dupeOptional(alloc, tmpl.match_mapping_type);
        errdefer if (match_mapping_type) |value| alloc.free(value);
        const scalar_type = try alloc.dupe(u8, scalar.?);
        errdefer alloc.free(scalar_type);
        try dynamic_rules.append(alloc, .{
            .name = name,
            .match = match,
            .unmatch = unmatch,
            .path_match = path_match,
            .path_unmatch = path_unmatch,
            .match_mapping_type = match_mapping_type,
            .scalar_type = scalar_type,
        });
    }

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

    try removeUnsafeStaticPaths(alloc, &fields, schema, &skipped_complex_fields);
    try qualifyCollidingFieldNames(alloc, fields.items);

    return .{
        .schema_version = schema.version,
        .fields = try fields.toOwnedSlice(alloc),
        .dynamic_rules = try dynamic_rules.toOwnedSlice(alloc),
        .skipped_dynamic_fields = skipped_dynamic_fields,
        .skipped_complex_fields = skipped_complex_fields,
        .skipped_unbounded_fields = skipped_unbounded_fields,
    };
}

/// Static projections are table-wide and do not carry a document-type
/// discriminator. If document schemas assign incompatible scalar types or any
/// schema can admit the same path as complex, unbounded, or unindexed,
/// projecting a bounded interpretation would make ingest depend on which
/// schema contributed the field spec. Omit every role for that ambiguous
/// physical path and leave it on the schemaless path-fact route.
const StaticPathResolution = union(enum) {
    /// The schema rejects this physical path, so it cannot conflict.
    absent,
    /// The schema admits the candidate role and scalar interpretation.
    compatible,
    /// The schema admits an unbounded, complex, unindexed, or ambiguous value.
    incompatible,
};

const ObjectSchemaView = struct {
    properties: []const schema_mod.DocumentProperty,
    pattern_properties: []const schema_mod.PatternProperty,
    additional_properties_allowed: ?bool,
    additional_properties_schema: ?*schema_mod.DocumentProperty,
    dynamic_infer_types: bool,
    unevaluated_properties_allowed: ?bool,
    unevaluated_properties_schema: ?*schema_mod.DocumentProperty,
    has_composition: bool,
};

const DynamicPathResolution = struct {
    resolution: StaticPathResolution = .absent,
    exhaustive: bool = false,
};

fn resolveDocumentStaticPath(
    schema: schema_mod.ParsedTableSchema,
    document_schema: schema_mod.DocumentSchema,
    path: []const u8,
    candidate: FieldCapability,
) anyerror!StaticPathResolution {
    return try resolveObjectStaticPath(
        schema,
        .{
            .properties = document_schema.properties,
            .pattern_properties = document_schema.pattern_properties,
            .additional_properties_allowed = document_schema.additional_properties_allowed,
            .additional_properties_schema = document_schema.additional_properties_schema,
            .dynamic_infer_types = document_schema.dynamic_infer_types,
            .unevaluated_properties_allowed = document_schema.unevaluated_properties_allowed,
            .unevaluated_properties_schema = document_schema.unevaluated_properties_schema,
            .has_composition = document_schema.any_of.len > 0 or
                document_schema.one_of.len > 0 or
                document_schema.all_of.len > 0 or
                document_schema.not_schema != null or
                document_schema.if_schema != null or
                document_schema.then_schema != null or
                document_schema.else_schema != null or
                document_schema.dependent_schemas.len > 0,
        },
        path,
        true,
        candidate,
    );
}

/// Resolve a candidate path with the same precedence used by schema
/// validation: explicit property, every matching pattern property, dynamic
/// template (at the document root), additionalProperties, then
/// unevaluated/default openness. Composition is conservatively incompatible:
/// its value-dependent intersections cannot be represented by a single
/// table-wide scalar fact without carrying the document type through ingest.
fn resolveObjectStaticPath(
    schema: schema_mod.ParsedTableSchema,
    object_schema: ObjectSchemaView,
    path: []const u8,
    allow_dynamic_templates: bool,
    candidate: FieldCapability,
) anyerror!StaticPathResolution {
    if (path.len == 0 or object_schema.has_composition) return .incompatible;

    const dot = std.mem.indexOfScalar(u8, path, '.');
    const field_name = if (dot) |idx| path[0..idx] else path;
    const tail: ?[]const u8 = if (dot) |idx| path[idx + 1 ..] else null;
    if (field_name.len == 0 or (tail != null and tail.?.len == 0)) return .incompatible;

    for (object_schema.properties) |property| {
        if (!std.mem.eql(u8, property.name, field_name)) continue;
        return resolvePropertyStaticPath(schema, property, tail, candidate);
    }

    var matched_pattern = false;
    var pattern_compatible = false;
    var pattern_incompatible = false;
    for (object_schema.pattern_properties) |pattern_property| {
        if (!try schema_mod.patternPropertyMatches(pattern_property.pattern, field_name)) continue;
        matched_pattern = true;
        switch (try resolvePropertyStaticPath(schema, pattern_property.property.*, tail, candidate)) {
            // Every matching pattern is an intersecting constraint. If one of
            // them makes the nested path impossible, the path is absent.
            .absent => return .absent,
            .incompatible => pattern_incompatible = true,
            .compatible => pattern_compatible = true,
        }
    }
    if (matched_pattern) {
        if (pattern_incompatible) return .incompatible;
        return if (pattern_compatible) .compatible else .absent;
    }

    var dynamic_resolution: DynamicPathResolution = .{};
    if (allow_dynamic_templates) {
        dynamic_resolution = resolveDynamicStaticPath(schema.dynamic_templates, field_name, tail, candidate);
        if (dynamic_resolution.exhaustive) return dynamic_resolution.resolution;
    }

    const fallback = try resolveObjectStaticFallback(schema, object_schema, tail, candidate);
    return mergeAlternativeResolutions(dynamic_resolution.resolution, fallback);
}

fn resolveObjectStaticFallback(
    schema: schema_mod.ParsedTableSchema,
    object_schema: ObjectSchemaView,
    tail: ?[]const u8,
    candidate: FieldCapability,
) anyerror!StaticPathResolution {
    if (object_schema.additional_properties_schema) |additional_schema| {
        return resolvePropertyStaticPath(schema, additional_schema.*, tail, candidate);
    }
    if (object_schema.additional_properties_allowed) |allowed| {
        return if (allowed) .incompatible else .absent;
    }
    // Dynamic inference is valid only with open additional properties, but
    // retain the guard here so a future parser relaxation fails closed.
    if (object_schema.dynamic_infer_types) return .incompatible;
    if (object_schema.unevaluated_properties_schema) |unevaluated_schema| {
        return resolvePropertyStaticPath(schema, unevaluated_schema.*, tail, candidate);
    }
    if (object_schema.unevaluated_properties_allowed) |allowed| {
        return if (allowed) .incompatible else .absent;
    }
    return if (schema.enforce_types) .absent else .incompatible;
}

fn resolvePropertyStaticPath(
    schema: schema_mod.ParsedTableSchema,
    property: schema_mod.DocumentProperty,
    tail: ?[]const u8,
    candidate: FieldCapability,
) anyerror!StaticPathResolution {
    if (property.antfly_index != null and !property.antfly_index.?) return .incompatible;
    if (propertyHasComposition(property)) return .incompatible;

    if (tail == null) {
        return if (propertySupportsStaticCapability(property, candidate)) .compatible else .incompatible;
    }

    // Static path lookup traverses objects only. Scalar and array declarations
    // therefore make a deeper dotted path impossible rather than ambiguous.
    if (property.item != null) return .absent;
    if (property.field_type) |field_type| {
        if (!std.mem.eql(u8, field_type, "object")) return .absent;
    } else if (scalarType(property) != null) {
        return .absent;
    }

    // A bare object (or unconstrained `{}` property) accepts arbitrary nested
    // members even when table-level enforce_types is enabled, because runtime
    // validation has no object-member rule to evaluate.
    if (!propertyHasObjectRules(property)) return .incompatible;

    return try resolveObjectStaticPath(
        schema,
        .{
            .properties = property.properties,
            .pattern_properties = property.pattern_properties,
            .additional_properties_allowed = property.additional_properties_allowed,
            .additional_properties_schema = property.additional_properties_schema,
            .dynamic_infer_types = property.dynamic_infer_types,
            .unevaluated_properties_allowed = property.unevaluated_properties_allowed,
            .unevaluated_properties_schema = property.unevaluated_properties_schema,
            .has_composition = false,
        },
        tail.?,
        false,
        candidate,
    );
}

fn propertySupportsStaticCapability(property: schema_mod.DocumentProperty, candidate: FieldCapability) bool {
    const scalar = boundedScalarForStaticProperty(property) orelse return false;
    return switch (candidate.role) {
        .group => isGroupType(scalar) and std.mem.eql(u8, candidate.scalar_type, scalar),
        .measure => isMeasureType(scalar) and std.mem.eql(u8, candidate.scalar_type, scalar),
        .time => std.mem.eql(u8, candidate.scalar_type, "datetime") and isTimeType(scalar, property.format),
    };
}

fn boundedScalarForStaticProperty(property: schema_mod.DocumentProperty) ?[]const u8 {
    if (property.antfly_index != null and !property.antfly_index.?) return null;
    if (property.item != null or property.properties.len > 0 or propertyHasComposition(property)) return null;
    return scalarType(property);
}

fn propertyHasComposition(property: schema_mod.DocumentProperty) bool {
    return property.root_ref or
        property.any_of.len > 0 or
        property.one_of.len > 0 or
        property.all_of.len > 0 or
        property.not_schema != null or
        property.if_schema != null or
        property.then_schema != null or
        property.else_schema != null or
        property.dependent_schemas.len > 0;
}

fn propertyHasObjectRules(property: schema_mod.DocumentProperty) bool {
    return property.properties.len > 0 or
        property.pattern_properties.len > 0 or
        property.required_fields.len > 0 or
        property.additional_properties_allowed != null or
        property.additional_properties_schema != null or
        property.dynamic_infer_types or
        property.unevaluated_properties_allowed != null or
        property.unevaluated_properties_schema != null or
        property.property_names != null or
        property.dependent_required.len > 0 or
        property.dependent_schemas.len > 0 or
        property.min_properties != null or
        property.max_properties != null or
        propertyHasComposition(property);
}

fn resolveDynamicStaticPath(
    templates: []const schema_mod.DynamicTemplate,
    field_name: []const u8,
    tail: ?[]const u8,
    candidate: FieldCapability,
) DynamicPathResolution {
    var result: DynamicPathResolution = .{};
    for (templates) |template| {
        if (!dynamicTemplateNamePathMatches(template, field_name)) continue;

        // A deeper static path requires the dynamically admitted root value to
        // be an object. Mapping-type constrained rules for other JSON shapes
        // cannot admit it.
        if (tail != null) {
            if (template.match_mapping_type) |mapping_type| {
                if (!std.mem.eql(u8, mapping_type, "object")) continue;
            }
            result.resolution = .incompatible;
        } else {
            const scalar = if (template.do_index orelse true) boundedScalarForTemplateType(template.field_type orelse "text") else null;
            result.resolution = mergeAlternativeResolutions(
                result.resolution,
                if (scalar) |bounded|
                    if (dynamicScalarSupportsCapability(bounded, candidate)) .compatible else .incompatible
                else
                    .incompatible,
            );
        }

        // Without a mapping-type selector this is the first matching template
        // for every possible value, so later templates and fallbacks are
        // unreachable under runtime first-match semantics.
        if (template.match_mapping_type == null) {
            result.exhaustive = true;
            break;
        }
    }
    return result;
}

fn dynamicScalarSupportsCapability(scalar: []const u8, candidate: FieldCapability) bool {
    if (!std.mem.eql(u8, scalar, candidate.scalar_type)) return false;
    return switch (candidate.role) {
        .group => isGroupType(scalar),
        .measure => isMeasureType(scalar),
        .time => std.mem.eql(u8, scalar, "datetime"),
    };
}

fn dynamicTemplateNamePathMatches(template: schema_mod.DynamicTemplate, path: []const u8) bool {
    const field_name = fieldNameFromPath(path);
    if (template.match_pattern) |pattern| {
        if (!schema_mod.globMatch(pattern, field_name)) return false;
    }
    if (template.unmatch_pattern) |pattern| {
        if (schema_mod.globMatch(pattern, field_name)) return false;
    }
    if (template.path_match) |pattern| {
        if (!schema_mod.globMatch(pattern, path)) return false;
    }
    if (template.path_unmatch) |pattern| {
        if (schema_mod.globMatch(pattern, path)) return false;
    }
    return true;
}

fn mergeAlternativeResolutions(lhs: StaticPathResolution, rhs: StaticPathResolution) StaticPathResolution {
    return switch (lhs) {
        .absent => rhs,
        .incompatible => .incompatible,
        .compatible => switch (rhs) {
            .absent => lhs,
            .incompatible => .incompatible,
            .compatible => .compatible,
        },
    };
}

const StaticPathSafety = struct {
    evaluated: u8 = 0,
    unsafe: u8 = 0,
};

fn capabilitySafetyBit(field: FieldCapability) ?u8 {
    const shift: u3 = switch (field.role) {
        .group => if (std.mem.eql(u8, field.scalar_type, "string"))
            0
        else if (std.mem.eql(u8, field.scalar_type, "boolean"))
            1
        else if (std.mem.eql(u8, field.scalar_type, "integer"))
            2
        else if (std.mem.eql(u8, field.scalar_type, "number"))
            3
        else if (std.mem.eql(u8, field.scalar_type, "datetime"))
            4
        else
            return null,
        .measure => if (std.mem.eql(u8, field.scalar_type, "integer"))
            5
        else if (std.mem.eql(u8, field.scalar_type, "number"))
            6
        else
            return null,
        .time => if (std.mem.eql(u8, field.scalar_type, "datetime")) 7 else return null,
    };
    return @as(u8, 1) << shift;
}

fn removeUnsafeStaticPaths(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(FieldCapability),
    schema: schema_mod.ParsedTableSchema,
    skipped_complex_fields: *u32,
) !void {
    if (fields.items.len == 0) return;
    const remove = try alloc.alloc(bool, fields.items.len);
    defer alloc.free(remove);
    @memset(remove, false);
    var path_safety = std.StringHashMapUnmanaged(StaticPathSafety).empty;
    defer path_safety.deinit(alloc);

    for (fields.items, 0..) |field, i| {
        if (schema.ttl_duration_ns > 0 and std.mem.eql(u8, field.path, schema.ttl_field)) {
            remove[i] = true;
            continue;
        }
        const bit = capabilitySafetyBit(field) orelse {
            remove[i] = true;
            continue;
        };
        const entry = try path_safety.getOrPut(alloc, field.path);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        if (entry.value_ptr.evaluated & bit != 0) {
            remove[i] = entry.value_ptr.unsafe & bit != 0;
            continue;
        }
        for (schema.document_schemas) |document_schema| {
            const resolution = try resolveDocumentStaticPath(schema, document_schema, field.path, field);
            switch (resolution) {
                .absent => {},
                .incompatible => {
                    remove[i] = true;
                    break;
                },
                .compatible => {},
            }
        }
        entry.value_ptr.evaluated |= bit;
        if (remove[i]) entry.value_ptr.unsafe |= bit;
    }

    for (fields.items, remove, 0..) |field, should_remove, i| {
        if (!should_remove) continue;
        var first_for_path = true;
        for (fields.items[0..i], remove[0..i]) |prior, prior_remove| {
            if (prior_remove and std.mem.eql(u8, prior.path, field.path)) {
                first_for_path = false;
                break;
            }
        }
        if (first_for_path) skipped_complex_fields.* +|= 1;
    }

    var write_idx: usize = 0;
    for (0..fields.items.len) |read_idx| {
        if (remove[read_idx]) {
            fields.items[read_idx].deinit(alloc);
            continue;
        }
        if (write_idx != read_idx) fields.items[write_idx] = fields.items[read_idx];
        write_idx += 1;
    }
    fields.items.len = write_idx;
}

/// Static facts need one table-wide identity per physical path. Preserve the
/// convenient leaf name while it is unambiguous, but qualify every colliding
/// leaf with its full dotted path so ordinary schemas such as billing.region +
/// shipping.region still produce a valid algebraic config.
fn qualifyCollidingFieldNames(alloc: Allocator, fields: []FieldCapability) !void {
    const collides = try alloc.alloc(bool, fields.len);
    defer alloc.free(collides);
    @memset(collides, false);
    for (fields, 0..) |field, i| {
        for (fields, 0..) |other, j| {
            if (i == j or std.mem.eql(u8, field.path, other.path)) continue;
            if (std.mem.eql(u8, field.name, other.name)) {
                collides[i] = true;
                break;
            }
        }
    }
    for (fields, collides) |*field, collision| {
        if (!collision or std.mem.eql(u8, field.name, field.path)) continue;
        const qualified = try alloc.dupe(u8, field.path);
        alloc.free(field.name);
        field.name = qualified;
    }
}

fn dupeOptional(alloc: Allocator, value: ?[]const u8) !?[]u8 {
    return if (value) |v| try alloc.dupe(u8, v) else null;
}

/// Map a dynamic-template Antfly field type onto the bounded algebraic scalar it
/// projects into, or null when the type is unbounded/unsupported (text, html,
/// search_as_you_type, embedding, geo, blob) and must stay schemaless.
fn boundedScalarForTemplateType(field_type: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, field_type, "keyword") or
        std.mem.eql(u8, field_type, "link") or
        std.mem.eql(u8, field_type, "string")) return "string";
    if (std.mem.eql(u8, field_type, "boolean") or std.mem.eql(u8, field_type, "bool")) return "boolean";
    if (std.mem.eql(u8, field_type, "datetime")) return "datetime";
    if (std.mem.eql(u8, field_type, "numeric") or std.mem.eql(u8, field_type, "number")) return "number";
    if (std.mem.eql(u8, field_type, "integer")) return "integer";
    return null;
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

    try out.append(alloc, ',');
    try appendJsonString(alloc, &out, "dynamic_field_rules");
    try out.append(alloc, ':');
    try appendDynamicRuleArray(alloc, &out, plan);

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
    for (plan.dynamic_rules) |rule| {
        try appendFmt(alloc, &canonical, "dyn:{s}:{s}:{s}:{s}:{s}:{s}:{s}|", .{
            rule.name,
            rule.scalar_type,
            rule.match orelse "",
            rule.unmatch orelse "",
            rule.path_match orelse "",
            rule.path_unmatch orelse "",
            rule.match_mapping_type orelse "",
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

fn appendDynamicRuleArray(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    plan: Plan,
) !void {
    try out.append(alloc, '[');
    for (plan.dynamic_rules, 0..) |rule, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, rule.name);
        try appendOptionalJsonField(alloc, out, "match", rule.match);
        try appendOptionalJsonField(alloc, out, "unmatch", rule.unmatch);
        try appendOptionalJsonField(alloc, out, "path_match", rule.path_match);
        try appendOptionalJsonField(alloc, out, "path_unmatch", rule.path_unmatch);
        try appendOptionalJsonField(alloc, out, "match_mapping_type", rule.match_mapping_type);
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, rule.scalar_type);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
}

fn appendOptionalJsonField(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    value: ?[]const u8,
) !void {
    const v = value orelse return;
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, key);
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, v);
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
    if (schema_mod.shouldIgnoreSchemaValidationField(property.name)) return;
    const explicitly_unindexed = property.antfly_index != null and !property.antfly_index.?;
    const composite = property.any_of.len > 0 or property.one_of.len > 0 or property.all_of.len > 0 or property.not_schema != null or property.if_schema != null;
    if (explicitly_unindexed) return;

    if (property.additional_properties_allowed orelse false) {
        skipped_dynamic_fields.* += 1;
        skipped_unbounded_fields.* += 1;
    }
    if (property.additional_properties_schema != null or property.pattern_properties.len > 0 or property.dynamic_infer_types) {
        skipped_dynamic_fields.* += 1;
        skipped_unbounded_fields.* += 1;
    }
    if (composite) skipped_complex_fields.* += 1;

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
    const owned_document_type = try alloc.dupe(u8, document_type);
    errdefer alloc.free(owned_document_type);
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_path = try alloc.dupe(u8, path);
    errdefer alloc.free(owned_path);
    const owned_scalar_type = try alloc.dupe(u8, scalar_type_value);
    errdefer alloc.free(owned_scalar_type);
    try fields.append(alloc, .{
        .document_type = owned_document_type,
        .name = owned_name,
        .path = owned_path,
        .scalar_type = owned_scalar_type,
        .role = role,
        .bounded = true,
        .dynamic_source = false,
    });
}

fn exerciseAppendCapabilityAllocation(alloc: Allocator) !void {
    var fields = std.ArrayListUnmanaged(FieldCapability).empty;
    defer {
        for (fields.items) |*field| field.deinit(alloc);
        fields.deinit(alloc);
    }
    try appendCapability(alloc, &fields, "document", "field", "nested.field", "string", .group);
}

test "capability construction is failure atomic" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseAppendCapabilityAllocation, .{});
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

test "schema capability qualifies colliding nested leaf names" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":8,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"billing":{"type":"object","properties":{"region":{"type":"keyword"}}},"shipping":{"type":"object","properties":{"region":{"type":"keyword"}}},"meta":{"type":"object","properties":{"tier":{"type":"keyword"}}}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    try expectCapability(plan, "doc", "billing.region", "billing.region", "string", .group);
    try expectCapability(plan, "doc", "shipping.region", "shipping.region", "string", .group);
    // Preserve the compact, backwards-compatible alias when the leaf is unique.
    try expectCapability(plan, "doc", "tier", "meta.tier", "string", .group);

    const config_json = try configJsonFromPlanAlloc(alloc, "orders", plan);
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try index_mod.validateConfig(parsed_config.value);
}

test "schema capability omits cross-document paths with incompatible scalar types" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"article","document_schemas":{"article":{"schema":{"type":"object","properties":{"status":{"type":"keyword"}},"additionalProperties":false}},"event":{"schema":{"type":"object","properties":{"status":{"type":"numeric"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    for (plan.fields) |field| try std.testing.expect(!std.mem.eql(u8, field.path, "status"));
    try std.testing.expectEqual(@as(u32, 1), plan.skipped_complex_fields);

    const config_json = try configJsonFromPlanAlloc(alloc, "events", plan);
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try index_mod.validateConfig(parsed_config.value);
    try std.testing.expectEqual(@as(usize, 0), parsed_config.value.group_fields.len);
    try std.testing.expectEqual(@as(usize, 0), parsed_config.value.measure_fields.len);
}

test "schema capability evaluates date formatted strings per algebraic role" {
    const alloc = std.testing.allocator;
    var mixed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"dated","document_schemas":{"dated":{"schema":{"type":"object","properties":{"published_at":{"type":"keyword","format":"date-time"}},"additionalProperties":false}},"plain":{"schema":{"type":"object","properties":{"published_at":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer mixed.deinit(alloc);

    var mixed_plan = try compilePlanAlloc(alloc, mixed);
    defer mixed_plan.deinit(alloc);
    try expectCapability(mixed_plan, "dated", "published_at", "published_at", "string", .group);
    try expectNoCapabilityRolePath(mixed_plan, "published_at", .time);

    var uniformly_dated = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"first","document_schemas":{"first":{"schema":{"type":"object","properties":{"published_at":{"type":"keyword","format":"date-time"}},"additionalProperties":false}},"second":{"schema":{"type":"object","properties":{"published_at":{"type":"keyword","format":"date"}},"additionalProperties":false}}}}
    );
    defer uniformly_dated.deinit(alloc);

    var dated_plan = try compilePlanAlloc(alloc, uniformly_dated);
    defer dated_plan.deinit(alloc);
    try expectCapability(dated_plan, "first", "published_at", "published_at", "string", .group);
    try expectCapability(dated_plan, "first", "published_at", "published_at", "datetime", .time);
}

test "schema capability suppresses ttl and schema ignored fields" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"dated","ttl_duration_ns":1000,"ttl_field":"expires_at","document_schemas":{"dated":{"schema":{"type":"object","properties":{"expires_at":{"type":"datetime"},"visible":{"type":"keyword"},"_private":{"type":"keyword"},"meta":{"type":"object","properties":{"public":{"type":"keyword"},"_private":{"type":"keyword"}},"additionalProperties":false}},"additionalProperties":false}},"plain":{"schema":{"type":"object","properties":{"expires_at":{"type":"datetime"},"visible":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    // TTL parsing intentionally accepts canonical integer timestamps even for
    // datetime declarations, while datetime fact projection accepts strings.
    try schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = "{\"_type\":\"plain\",\"expires_at\":1700000000000000000,\"visible\":\"yes\"}" }});

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    try expectNoCapabilityPath(plan, "expires_at");
    try expectNoCapabilityPath(plan, "_private");
    try expectNoCapabilityPath(plan, "meta._private");
    try expectCapability(plan, "dated", "visible", "visible", "string", .group);
    try expectCapability(plan, "dated", "public", "meta.public", "string", .group);
}

test "schema capability omits bounded paths claimed as unbounded or complex by another document type" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":11,"default_type":"article","document_schemas":{"article":{"schema":{"type":"object","properties":{"status":{"type":"keyword"},"metadata":{"type":"keyword"},"tags":{"type":"keyword"}},"additionalProperties":false}},"event":{"schema":{"type":"object","properties":{"status":{"type":"text"},"metadata":{"type":"object","properties":{"code":{"type":"keyword"}}},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    for (plan.fields) |field| {
        try std.testing.expect(!std.mem.eql(u8, field.path, "status"));
        try std.testing.expect(!std.mem.eql(u8, field.path, "metadata"));
        try std.testing.expect(!std.mem.eql(u8, field.path, "tags"));
    }

    const config_json = try configJsonFromPlanAlloc(alloc, "mixed", plan);
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try index_mod.validateConfig(parsed_config.value);
    try std.testing.expectEqual(@as(usize, 1), parsed_config.value.group_fields.len);
    try std.testing.expectEqualStrings("metadata.code", parsed_config.value.group_fields[0].path);
}

test "schema capability omits paths admitted by open and dynamic document schemas" {
    const alloc = std.testing.allocator;
    const cases = [_][]const u8{
        // Explicitly open root.
        "{\"version\":12,\"default_type\":\"article\",\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true}}}}",
        // Inferred dynamic fields are value-dependent and therefore unbounded.
        "{\"version\":12,\"default_type\":\"article\",\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}",
        // Default-open root when type enforcement is disabled.
        "{\"version\":12,\"default_type\":\"article\",\"enforce_types\":false,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\"}}}}",
        // A matching pattern assigns an incompatible bounded type.
        "{\"version\":12,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"patternProperties\":{\"^status$\":{\"type\":\"numeric\"}},\"additionalProperties\":false}}}}",
        // A typed additionalProperties schema assigns an incompatible type.
        "{\"version\":12,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"numeric\"}}}}}",
        // unevaluatedProperties is another path-admission fallback.
        "{\"version\":12,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"unevaluatedProperties\":true}}}}",
        // A global template is evaluated before a closed additionalProperties
        // fallback and can therefore admit the otherwise undeclared path.
        "{\"version\":12,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false}}},\"dynamic_templates\":[{\"name\":\"numeric_status\",\"match\":\"status\",\"mapping\":{\"type\":\"numeric\"}}]}",
    };

    for (cases) |schema_json| {
        var parsed = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
        defer parsed.deinit(alloc);
        var plan = try compilePlanAlloc(alloc, parsed);
        defer plan.deinit(alloc);
        try expectNoCapabilityPath(plan, "status");
    }
}

test "schema capability omits nested paths admitted by open object schemas" {
    const alloc = std.testing.allocator;
    const cases = [_][]const u8{
        "{\"version\":13,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"additionalProperties\":true}},\"additionalProperties\":false}}}}",
        // A bare object has no member-validation rule and remains open even
        // under table-level type enforcement.
        "{\"version\":13,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\"}},\"additionalProperties\":false}}}}",
        "{\"version\":13,\"default_type\":\"article\",\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"properties\":{\"status\":{\"type\":\"keyword\"}},\"additionalProperties\":false}},\"additionalProperties\":false}},\"event\":{\"schema\":{\"type\":\"object\",\"properties\":{\"meta\":{\"type\":\"object\",\"patternProperties\":{\"^status$\":{\"type\":\"numeric\"}},\"additionalProperties\":false}},\"additionalProperties\":false}}}}",
    };

    for (cases) |schema_json| {
        var parsed = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
        defer parsed.deinit(alloc);
        var plan = try compilePlanAlloc(alloc, parsed);
        defer plan.deinit(alloc);
        try expectNoCapabilityPath(plan, "meta.status");
    }
}

test "schema capability treats min properties only objects as closed when types are enforced" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":13,"default_type":"article","enforce_types":true,"document_schemas":{"article":{"schema":{"type":"object","properties":{"meta":{"type":"object","properties":{"status":{"type":"keyword"}},"additionalProperties":false}},"additionalProperties":false}},"event":{"schema":{"type":"object","properties":{"meta":{"type":"object","minProperties":0}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    try expectCapability(plan, "article", "status", "meta.status", "string", .group);
}

test "schema capability preserves compatible typed open path declarations" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":14,"default_type":"explicit","enforce_types":true,
        \\"document_schemas":{
        \\"explicit":{"schema":{"type":"object","properties":{"status":{"type":"keyword"},"meta":{"type":"object","properties":{"status":{"type":"keyword"}},"additionalProperties":false}},"additionalProperties":false}},
        \\"additional":{"schema":{"type":"object","properties":{"meta":{"type":"object","additionalProperties":{"type":"keyword"}}},"additionalProperties":{"type":"keyword"}}},
        \\"pattern":{"schema":{"type":"object","patternProperties":{"^status$":{"type":"keyword"}},"properties":{"meta":{"type":"object","patternProperties":{"^status$":{"type":"keyword"}},"additionalProperties":false}},"additionalProperties":false}},
        \\"unevaluated":{"schema":{"type":"object","unevaluatedProperties":{"type":"keyword"}}},
        \\"template":{"schema":{"type":"object","additionalProperties":false}}},
        \\"dynamic_templates":[{"name":"status_keyword","match":"status","mapping":{"type":"keyword"}}]
        \\}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    try expectCapability(plan, "explicit", "status", "status", "string", .group);
    try expectCapability(plan, "explicit", "meta.status", "meta.status", "string", .group);
}

test "schema capability coalesces compatible paths shared by document types" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":10,"default_type":"article","document_schemas":{"article":{"schema":{"type":"object","properties":{"status":{"type":"keyword"}},"additionalProperties":false}},"event":{"schema":{"type":"object","properties":{"status":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    const config_json = try configJsonFromPlanAlloc(alloc, "events", plan);
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try index_mod.validateConfig(parsed_config.value);
    try std.testing.expectEqual(@as(usize, 1), parsed_config.value.group_fields.len);
    try std.testing.expectEqualStrings("status", parsed_config.value.group_fields[0].path);
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

test "schema capability compiles bounded dynamic templates into runtime rules" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"doc",
        \\"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},
        \\"dynamic_templates":[
        \\{"name":"ids_as_keyword","match":"*_id","mapping":{"type":"keyword"}},
        \\{"name":"metrics_as_numeric","path_match":"metrics.*","mapping":{"type":"numeric"}},
        \\{"name":"events_as_date","match_mapping_type":"date","mapping":{"type":"datetime"}},
        \\{"name":"bodies_as_text","match":"body_*","mapping":{"type":"text"}},
        \\{"name":"only_negative","unmatch":"skip_*","mapping":{"type":"keyword"}}
        \\]}
    );
    defer parsed.deinit(alloc);

    var plan = try compilePlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    // Only templates with a name/path selector AND a bounded type become rules:
    // ids (keyword/match) and metrics (numeric/path_match) => 2 rules. Skipped:
    // the date template (match_mapping_type-only — can't resolve at query time),
    // the text template (unbounded), and the negative-only template.
    try std.testing.expectEqual(@as(usize, 2), plan.dynamic_rules.len);
    try std.testing.expect(plan.skipped_unbounded_fields >= 3);
    for (plan.dynamic_rules) |rule| {
        try std.testing.expect(!std.mem.eql(u8, rule.name, "events_as_date"));
    }

    const config_json = try configJsonFromPlanAlloc(alloc, "orders", plan);
    defer alloc.free(config_json);
    var parsed_config = try std.json.parseFromSlice(index_mod.Config, alloc, config_json, .{ .allocate = .alloc_always });
    defer parsed_config.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_config.value.dynamic_field_rules.len);
    // The emitted config must satisfy the index validator (selector present,
    // bounded scalar type) so the index can open against it.
    try index_mod.validateConfig(parsed_config.value);

    var found_numeric = false;
    for (parsed_config.value.dynamic_field_rules) |rule| {
        if (std.mem.eql(u8, rule.name, "metrics_as_numeric")) {
            try std.testing.expectEqualStrings("metrics.*", rule.path_match.?);
            try std.testing.expectEqualStrings("number", rule.type);
            found_numeric = true;
        }
    }
    try std.testing.expect(found_numeric);
}

test "schema capability dynamic template compilation is allocation failure atomic" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":9,"default_type":"doc","dynamic_templates":[
        \\{"name":"rich_rule","match":"*_id","unmatch":"private_*","path_match":"items.*","path_unmatch":"items.private.*","match_mapping_type":"string","mapping":{"type":"keyword"}},
        \\{"name":"metric_rule","path_match":"metrics.*","mapping":{"type":"numeric"}}
        \\]}
    );
    defer parsed.deinit(alloc);

    const Runner = struct {
        fn run(failing_alloc: Allocator, schema: schema_mod.ParsedTableSchema) !void {
            var plan = try compilePlanAlloc(failing_alloc, schema);
            defer plan.deinit(failing_alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{parsed});
}

test "schema capability fingerprint reflects dynamic template type change" {
    const alloc = std.testing.allocator;
    var keyword_schema = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},"dynamic_templates":[{"name":"ext","match":"ext_*","mapping":{"type":"keyword"}}]}
    );
    defer keyword_schema.deinit(alloc);
    var numeric_schema = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}}}}},"dynamic_templates":[{"name":"ext","match":"ext_*","mapping":{"type":"numeric"}}]}
    );
    defer numeric_schema.deinit(alloc);

    var keyword_plan = try compilePlanAlloc(alloc, keyword_schema);
    defer keyword_plan.deinit(alloc);
    var numeric_plan = try compilePlanAlloc(alloc, numeric_schema);
    defer numeric_plan.deinit(alloc);

    const keyword_fp = try capabilityFingerprintAlloc(alloc, keyword_plan);
    defer alloc.free(keyword_fp);
    const numeric_fp = try capabilityFingerprintAlloc(alloc, numeric_plan);
    defer alloc.free(numeric_fp);

    // A template-only type change (same schema version) must still shift the
    // capability fingerprint so the sidecar detects drift.
    try std.testing.expect(!std.mem.eql(u8, keyword_fp, numeric_fp));
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
    required: bool = false,
    allows_null: bool = false,
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
    column_indexes: std.StringHashMapUnmanaged(usize) = .empty,
    skipped_complex_fields: u32 = 0,
    skipped_dynamic_fields: u32 = 0,

    pub fn deinit(self: *RelationalPlan, alloc: Allocator) void {
        self.column_indexes.deinit(alloc);
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

    var column_indexes = std.StringHashMapUnmanaged(usize).empty;
    errdefer column_indexes.deinit(alloc);
    try column_indexes.ensureTotalCapacity(alloc, @intCast(columns.items.len));
    for (columns.items, 0..) |column, index| {
        column_indexes.putAssumeCapacity(column.name, index);
    }

    return .{
        .schema_version = schema.version,
        .relational = schema.storage_mode == .relational,
        .columns = try columns.toOwnedSlice(alloc),
        .column_indexes = column_indexes,
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
        try appendJsonString(alloc, &out, "required");
        try out.appendSlice(alloc, if (column.required) ":true," else ":false,");
        try appendJsonString(alloc, &out, "allows_null");
        try out.appendSlice(alloc, if (column.allows_null) ":true," else ":false,");
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
    const owned_document_type = try alloc.dupe(u8, document_type);
    errdefer alloc.free(owned_document_type);
    const owned_name = try alloc.dupe(u8, property.name);
    errdefer alloc.free(owned_name);
    const owned_path = try alloc.dupe(u8, property.name);
    errdefer alloc.free(owned_path);
    const owned_column_type = try alloc.dupe(u8, column_type);
    errdefer alloc.free(owned_column_type);
    const owned_physical = try alloc.dupe(u8, physicalForColumnType(column_type));
    errdefer alloc.free(owned_physical);
    const allows_null = schema_mod.documentPropertyAllowsNull(property);
    try columns.append(alloc, .{
        .document_type = owned_document_type,
        .name = owned_name,
        .path = owned_path,
        .column_type = owned_column_type,
        .physical = owned_physical,
        .required = required,
        .allows_null = allows_null,
        .nullable = !required or allows_null,
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
    if (std.mem.eql(u8, column_type, "integer")) return "i64_val";
    if (std.mem.eql(u8, column_type, "datetime")) return "u64_val";
    if (std.mem.eql(u8, column_type, "number")) return "f64_val";
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
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"qty":{"type":"integer"},"created_at":{"type":"datetime"},"active":{"type":"boolean"},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"tags":{"type":"array","items":{"type":"keyword"}},"payload":{"type":"json"}},"required":["id","amount"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);

    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.relational);
    try std.testing.expectEqual(@as(u32, 4), plan.schema_version);
    try std.testing.expectEqual(@as(usize, 8), plan.columns.len);
    try std.testing.expectEqual(@as(u32, 0), plan.skipped_complex_fields);
    try std.testing.expectEqual(@as(u32, 0), plan.skipped_dynamic_fields);

    try expectRelationalColumn(plan, "row", "id", "string", "bytes_val", false, false);
    try expectRelationalColumn(plan, "row", "amount", "number", "f64_val", false, false);
    try expectRelationalColumn(plan, "row", "qty", "integer", "i64_val", true, false);
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

fn exerciseRelationalColumnPlanAllocation(alloc: Allocator, schema: schema_mod.ParsedTableSchema) !void {
    var plan = try relationalColumnPlanAlloc(alloc, schema);
    defer plan.deinit(alloc);
}

test "relational column plan construction is failure atomic" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"payload":{"type":"json"}},"required":["id"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    try std.testing.checkAllAllocationFailures(alloc, exerciseRelationalColumnPlanAllocation, .{parsed});
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
//   - a missing required value -> error.MissingRequiredColumn
//   - an explicit null not admitted by the JSON Schema -> error.InvalidColumnValue
//   - an undeclared non-metadata field -> error.UnknownColumn
//   - value that does not match the declared column type -> error.InvalidColumnValue
//   - json columns are stringified to bytes (and flagged is_json so the caller
//     can additionally index the subtree like a document)
//
// Numeric physical encoding matches typed_doc_values:
//   - number   -> f64 (native)
//   - integer  -> i64 (exact signed round-trip, including integral JSON floats)
//   - datetime -> u64 epoch nanoseconds (epoch integers/integer-strings and
//                 RFC3339/date-only strings are accepted)
//   - boolean  -> bool, geopoint -> packed lat/lon, string/blob/geoshape -> bytes
// ---------------------------------------------------------------------------

const typed_doc_values = @import("../../../section/typed_doc_values.zig");

pub const PhysicalType = enum { u64_val, i64_val, f64_val, bytes_val, bool_val, geo_point };

pub const GeoPoint = struct { lat: f64, lon: f64 };

pub const ColumnValue = union(PhysicalType) {
    u64_val: u64,
    i64_val: i64,
    f64_val: f64,
    bytes_val: []const u8,
    bool_val: bool,
    geo_point: GeoPoint,
};

pub const RelationalCell = struct {
    column: usize,
    present: bool = false,
    is_null: bool = false,
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
        if (column_index >= self.cells.len) return null;
        const candidate = self.cells[column_index];
        if (candidate.column != column_index or !candidate.present) return null;
        return candidate;
    }
};

pub fn typedValue(value: ColumnValue) typed_doc_values.TypedValue {
    return switch (value) {
        .u64_val => |v| .{ .u64_val = v },
        .i64_val => |v| .{ .i64_val = v },
        .f64_val => |v| .{ .f64_val = v },
        .bytes_val => |v| .{ .bytes_val = v },
        .bool_val => |v| .{ .bool_val = v },
        .geo_point => |v| .{ .geo_point = .{ .lat = v.lat, .lon = v.lon } },
    };
}

pub fn projectRelationalRowAlloc(alloc: Allocator, plan: RelationalPlan, root: std.json.Value) !RelationalRow {
    if (root != .object) return error.NotAnObject;

    var fields = root.object.iterator();
    while (fields.next()) |entry| {
        if (schema_mod.shouldIgnoreSchemaValidationField(entry.key_ptr.*)) continue;
        if (!plan.column_indexes.contains(entry.key_ptr.*)) return error.UnknownColumn;
    }

    const cells = try alloc.alloc(RelationalCell, plan.columns.len);
    errdefer alloc.free(cells);
    var pool = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (pool.items) |buffer| alloc.free(buffer);
        pool.deinit(alloc);
    }

    for (plan.columns, 0..) |column, i| {
        cells[i] = .{ .column = i, .present = false, .is_json = column.is_json };
        const found = root.object.get(column.name);
        if (found == null) {
            if (column.required) return error.MissingRequiredColumn;
            continue;
        }
        if (found.? == .null) {
            if (!column.allows_null) return error.InvalidColumnValue;
            cells[i] = .{ .column = i, .present = true, .is_null = true, .is_json = column.is_json };
            continue;
        }
        const coerced = (try coerceColumnValue(alloc, column.column_type, found.?)) orelse return error.InvalidColumnValue;
        if (coerced.owned) |buffer| {
            pool.append(alloc, buffer) catch |err| {
                alloc.free(buffer);
                return err;
            };
        }
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
    if (std.mem.eql(u8, column_type, "number")) {
        const number = schema_mod.documentNumberToF64(json_value) orelse return null;
        return Coerced{ .value = .{ .f64_val = number } };
    }
    if (std.mem.eql(u8, column_type, "integer")) {
        const number = schema_mod.documentIntegerToI64(json_value) orelse return null;
        return Coerced{ .value = .{ .i64_val = number } };
    }
    if (std.mem.eql(u8, column_type, "datetime")) {
        const timestamp = schema_mod.documentDateTimeToNs(json_value) orelse return null;
        return Coerced{ .value = .{ .u64_val = timestamp } };
    }
    if (std.mem.eql(u8, column_type, "geopoint")) {
        const point = geoPointFromJson(json_value) orelse return null;
        return Coerced{ .value = .{ .geo_point = point } };
    }
    return null;
}

fn geoPointFromJson(json_value: std.json.Value) ?GeoPoint {
    switch (json_value) {
        .object => |object| {
            if (object.count() != 2) return null;
            const lat = jsonNumber(object.get("lat") orelse return null) orelse return null;
            const lon = jsonNumber(object.get("lon") orelse return null) orelse return null;
            if (!geo_mod.latitudeIsValid(lat) or !geo_mod.longitudeIsValid(lon)) return null;
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

fn relationalTestPlanAlloc(alloc: Allocator) !RelationalPlan {
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"version":4,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"qty":{"type":"integer"},"ts":{"type":"datetime"},"active":{"type":"boolean"},"location":{"type":"geopoint"},"attrs":{"type":"object","properties":{"k":{"type":"keyword"}}},"payload":{"type":"json"}},"required":["id","amount"],"additionalProperties":false}}}}
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

    var invalid_latitude = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a","amount":1.0,"location":{"lat":91,"lon":0}}
    , .{});
    defer invalid_latitude.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, invalid_latitude.value));

    var invalid_longitude = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a","amount":1.0,"location":{"lat":0,"lon":181}}
    , .{});
    defer invalid_longitude.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, invalid_longitude.value));

    var lossy_extra_member = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a","amount":1.0,"location":{"lat":0,"lon":0,"altitude":10}}
    , .{});
    defer lossy_extra_member.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, lossy_extra_member.value));

    var undeclared = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a","amount":1.0,"undeclared":true}
    , .{});
    defer undeclared.deinit();
    try std.testing.expectError(error.UnknownColumn, projectRelationalRowAlloc(alloc, plan, undeclared.value));

    // Reserved metadata is intentionally not a relational column, matching
    // schema validation's treatment of top-level underscore-prefixed fields.
    var metadata = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"_id":"row-a","id":"a","amount":1.0}
    , .{});
    defer metadata.deinit();
    var metadata_row = try projectRelationalRowAlloc(alloc, plan, metadata.value);
    defer metadata_row.deinit(alloc);
    try std.testing.expectEqualStrings("a", metadata_row.cell(relationalColumnIndex(plan, "id").?).?.value.bytes_val);
}

test "relational integer validation and projection share exact i64 conversion" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"qty":{"type":"integer","maximum":9007199254740992,"multipleOf":2}},"required":["qty"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);
    const qty_index = relationalColumnIndex(plan, "qty").?;

    const accepted =
        \\{"qty":9007199254740992}
    ;
    try schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = accepted }});
    var accepted_value = try std.json.parseFromSlice(std.json.Value, alloc, accepted, .{});
    defer accepted_value.deinit();
    var row = try projectRelationalRowAlloc(alloc, plan, accepted_value.value);
    defer row.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 9_007_199_254_740_992), row.cell(qty_index).?.value.i64_val);

    const constraint_violations = [_][]const u8{
        // Both values collapse onto adjacent f64 representations, so these
        // checks must use the exact relational i64 value and schema token.
        "{\"qty\":9007199254740993}",
        "{\"qty\":9007199254740991}",
    };
    for (constraint_violations) |document| {
        try std.testing.expectError(
            error.InvalidBatchRequest,
            schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = document }}),
        );
    }

    const invalid_documents = [_][]const u8{
        "{\"qty\":\"5\"}",
        "{\"qty\":5.5}",
        "{\"qty\":9223372036854775808.0}",
    };
    for (invalid_documents) |document| {
        try std.testing.expectError(
            error.InvalidBatchRequest,
            schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = document }}),
        );
        var value = try std.json.parseFromSlice(std.json.Value, alloc, document, .{});
        defer value.deinit();
        try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, value.value));
    }
}

test "relational number validation rejects values the row codec cannot encode" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"amount":{"type":"numeric"}},"required":["amount"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const overflowed_number =
        \\{"amount":1e5000}
    ;
    try std.testing.expectError(
        error.InvalidBatchRequest,
        schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = overflowed_number }}),
    );
    var value = try std.json.parseFromSlice(std.json.Value, alloc, overflowed_number, .{});
    defer value.deinit();
    try std.testing.expect(value.value.object.get("amount").? == .number_string);
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, value.value));
}

test "relational projection preserves explicit null separately from absence" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"required_nullable":{"type":["keyword","null"]},"optional_nullable":{"type":["keyword","null"]},"optional_nonnull":{"type":"keyword"}},"required":["required_nullable"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const required_index = relationalColumnIndex(plan, "required_nullable").?;
    const optional_index = relationalColumnIndex(plan, "optional_nullable").?;
    try std.testing.expect(plan.columns[required_index].required);
    try std.testing.expect(plan.columns[required_index].allows_null);
    try std.testing.expect(plan.columns[required_index].nullable);

    var explicit_nulls = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"required_nullable":null,"optional_nullable":null}
    , .{});
    defer explicit_nulls.deinit();
    var row = try projectRelationalRowAlloc(alloc, plan, explicit_nulls.value);
    defer row.deinit(alloc);
    try std.testing.expect(row.cell(required_index).?.is_null);
    try std.testing.expect(row.cell(optional_index).?.is_null);

    var absent_optional = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"required_nullable":"present"}
    , .{});
    defer absent_optional.deinit();
    var sparse_row = try projectRelationalRowAlloc(alloc, plan, absent_optional.value);
    defer sparse_row.deinit(alloc);
    try std.testing.expect(sparse_row.cell(optional_index) == null);

    var missing_required = try std.json.parseFromSlice(std.json.Value, alloc, "{}", .{});
    defer missing_required.deinit();
    try std.testing.expectError(error.MissingRequiredColumn, projectRelationalRowAlloc(alloc, plan, missing_required.value));

    var forbidden_null = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"required_nullable":"present","optional_nonnull":null}
    , .{});
    defer forbidden_null.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, forbidden_null.value));
}

test "relational projection shares composed null semantics with schema validation" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"enum_nullable":{"type":"keyword","enum":["ready",null]},"const_nullable":{"type":"keyword","const":null},"any_nullable":{"type":"keyword","anyOf":[{"const":null},{"const":"ready"}]},"one_nullable":{"type":"keyword","oneOf":[{"const":null},{"const":"ready"}]},"not_nullable":{"type":["keyword","null"],"not":{"const":null}}},"required":["enum_nullable","const_nullable","any_nullable","one_nullable"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const accepted =
        \\{"enum_nullable":null,"const_nullable":null,"any_nullable":null,"one_nullable":null}
    ;
    try schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = accepted }});
    var accepted_value = try std.json.parseFromSlice(std.json.Value, alloc, accepted, .{});
    defer accepted_value.deinit();
    var row = try projectRelationalRowAlloc(alloc, plan, accepted_value.value);
    defer row.deinit(alloc);

    for ([_][]const u8{ "enum_nullable", "const_nullable", "any_nullable", "one_nullable" }) |name| {
        const column_index = relationalColumnIndex(plan, name).?;
        try std.testing.expect(plan.columns[column_index].allows_null);
        try std.testing.expect(row.cell(column_index).?.is_null);
    }

    const rejected =
        \\{"enum_nullable":null,"const_nullable":null,"any_nullable":null,"one_nullable":null,"not_nullable":null}
    ;
    try std.testing.expectError(
        error.InvalidBatchRequest,
        schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = rejected }}),
    );
    var rejected_value = try std.json.parseFromSlice(std.json.Value, alloc, rejected, .{});
    defer rejected_value.deinit();
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, rejected_value.value));
}

test "relational projection preserves literal top-level property names" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"first.name":{"type":"keyword"}},"required":["first.name"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const accepted =
        \\{"first.name":"Ada"}
    ;
    try schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = accepted }});
    var accepted_value = try std.json.parseFromSlice(std.json.Value, alloc, accepted, .{});
    defer accepted_value.deinit();
    var row = try projectRelationalRowAlloc(alloc, plan, accepted_value.value);
    defer row.deinit(alloc);

    const column_index = relationalColumnIndex(plan, "first.name").?;
    try std.testing.expectEqualStrings("Ada", row.cell(column_index).?.value.bytes_val);
    try std.testing.expect(row.cell(plan.columns.len) == null);

    var nested = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"first":{"name":"Ada"}}
    , .{});
    defer nested.deinit();
    try std.testing.expectError(error.UnknownColumn, projectRelationalRowAlloc(alloc, plan, nested.value));
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
    try std.testing.expectEqual(@as(i64, 7), qty.value.i64_val);
    const ts = row.cell(relationalColumnIndex(plan, "ts").?).?;
    try std.testing.expectEqual(@as(u64, 1000), ts.value.u64_val);
    const active = row.cell(relationalColumnIndex(plan, "active").?).?;
    try std.testing.expect(active.value.bool_val);

    const encoded = try relational_row_codec.serialize(alloc, &.{
        .{ .path = "qty", .value_type = .i64_val, .value = typedValue(qty.value) },
        .{ .path = "ts", .value_type = .u64_val, .value = typedValue(ts.value) },
    });
    defer alloc.free(encoded);
    const reconstructed = try relational_row_codec.reconstructValueAlloc(alloc, encoded);
    defer alloc.free(reconstructed);
    try std.testing.expectEqualStrings("{\"qty\":7,\"ts\":1000}", reconstructed);

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

fn exerciseRelationalProjectionAllocation(alloc: Allocator, plan: RelationalPlan, root: std.json.Value) !void {
    var row = try projectRelationalRowAlloc(alloc, plan, root);
    defer row.deinit(alloc);
}

test "relational row projection is failure atomic" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);
    var doc = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"abc","amount":12.5,"attrs":{"k":"v"},"payload":[1,2,3]}
    , .{});
    defer doc.deinit();
    try std.testing.checkAllAllocationFailures(alloc, exerciseRelationalProjectionAllocation, .{ plan, doc.value });
}

test "relational integer and datetime columns preserve their logical values" {
    const alloc = std.testing.allocator;
    var plan = try relationalTestPlanAlloc(alloc);
    defer plan.deinit(alloc);
    const qty_index = relationalColumnIndex(plan, "qty").?;

    var negative = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"a","amount":0.0,"qty":-5}
    , .{});
    defer negative.deinit();
    var positive = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"b","amount":0.0,"qty":5,"ts":"1970-01-01T00:00:00.000000015Z"}
    , .{});
    defer positive.deinit();
    var date_only = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"c","amount":0.0,"ts":"1970-01-01"}
    , .{});
    defer date_only.deinit();
    var invalid_text = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"d","amount":0.0,"ts":"not-a-date"}
    , .{});
    defer invalid_text.deinit();
    var invalid_negative = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"id":"e","amount":0.0,"ts":-1}
    , .{});
    defer invalid_negative.deinit();

    var negative_row = try projectRelationalRowAlloc(alloc, plan, negative.value);
    defer negative_row.deinit(alloc);
    var positive_row = try projectRelationalRowAlloc(alloc, plan, positive.value);
    defer positive_row.deinit(alloc);
    var date_only_row = try projectRelationalRowAlloc(alloc, plan, date_only.value);
    defer date_only_row.deinit(alloc);

    try std.testing.expectEqual(@as(i64, -5), negative_row.cell(qty_index).?.value.i64_val);
    try std.testing.expectEqual(@as(i64, 5), positive_row.cell(qty_index).?.value.i64_val);
    try std.testing.expectEqual(
        @as(u64, 15),
        positive_row.cell(relationalColumnIndex(plan, "ts").?).?.value.u64_val,
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        date_only_row.cell(relationalColumnIndex(plan, "ts").?).?.value.u64_val,
    );
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, invalid_text.value));
    try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, invalid_negative.value));
}

test "relational datetime validation and projection reject invalid or unencodable values" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"ts":{"type":"datetime"}},"required":["ts"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const invalid_documents = [_][]const u8{
        "{\"ts\":\"2023-02-29\"}",
        "{\"ts\":\"2024-13-01\"}",
        "{\"ts\":\"2024-01-01T24:00:00Z\"}",
        "{\"ts\":\"9999-12-31\"}",
    };
    for (invalid_documents) |document| {
        try std.testing.expectError(
            error.InvalidBatchRequest,
            schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = document }}),
        );
        var value = try std.json.parseFromSlice(std.json.Value, alloc, document, .{});
        defer value.deinit();
        try std.testing.expectError(error.InvalidColumnValue, projectRelationalRowAlloc(alloc, plan, value.value));
    }
}

test "relational custom ttl column validates and projects" {
    const alloc = std.testing.allocator;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc,
        \\{"storage_mode":"relational","ttl_duration_ns":1,"ttl_field":"_expires_at","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"_expires_at":{"type":"datetime"}},"required":["id","_expires_at"],"additionalProperties":false}}}}
    );
    defer parsed.deinit(alloc);
    var plan = try relationalColumnPlanAlloc(alloc, parsed);
    defer plan.deinit(alloc);

    const document =
        \\{"id":"a","_expires_at":"1970-01-01T00:00:00Z"}
    ;
    try schema_mod.validateWritesAgainstTableSchema(alloc, parsed, &.{.{ .value = document }});
    var value = try std.json.parseFromSlice(std.json.Value, alloc, document, .{});
    defer value.deinit();
    var row = try projectRelationalRowAlloc(alloc, plan, value.value);
    defer row.deinit(alloc);

    const ttl_index = relationalColumnIndex(plan, "_expires_at") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), row.cell(ttl_index).?.value.u64_val);
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

    // Drive the real typed_doc_values writer/reader for each typed-scan column.
    try expectTypedColumnRoundTrip(alloc, .i64_val, typedValue(row.cell(relationalColumnIndex(plan, "qty").?).?.value));
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
        .i64_val => try std.testing.expectEqual(value.i64_val, (try reader.getI64(0)).?),
        .f64_val => try std.testing.expectEqual(value.f64_val, (try reader.getF64(0)).?),
        .bool_val => try std.testing.expectEqual(value.bool_val, (try reader.getBool(0)).?),
        else => unreachable,
    }
}

fn expectNoCapabilityPath(plan: Plan, path: []const u8) !void {
    for (plan.fields) |field| {
        if (std.mem.eql(u8, field.path, path)) return error.UnexpectedCapability;
    }
}

fn expectNoCapabilityRolePath(plan: Plan, path: []const u8, role: FieldRole) !void {
    for (plan.fields) |field| {
        if (field.role == role and std.mem.eql(u8, field.path, path)) return error.UnexpectedCapability;
    }
}
