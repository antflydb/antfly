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

/// One document schema's declaration for a physical path. A null scalar marks
/// a shape that cannot participate in the bounded, table-wide projection
/// (object, array, unbounded text, composite schema, or explicitly unindexed).
const StaticPathClaim = struct {
    path: []u8,
    bounded_scalar_type: ?[]const u8,

    fn deinit(self: *StaticPathClaim, alloc: Allocator) void {
        alloc.free(self.path);
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
    var static_path_claims = std.ArrayListUnmanaged(StaticPathClaim).empty;
    defer {
        for (static_path_claims.items) |*claim| claim.deinit(alloc);
        static_path_claims.deinit(alloc);
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
                &static_path_claims,
                &skipped_dynamic_fields,
                &skipped_complex_fields,
                &skipped_unbounded_fields,
            );
        }
    }

    try removeConflictingStaticPaths(alloc, &fields, static_path_claims.items, &skipped_complex_fields);
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
/// schema declares the same path complex, unbounded, or unindexed, projecting a
/// bounded interpretation would make ingest depend on which schema contributed
/// the field spec. Omit every role for that ambiguous physical path and leave it
/// on the schemaless path-fact route.
fn removeConflictingStaticPaths(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged(FieldCapability),
    claims: []const StaticPathClaim,
    skipped_complex_fields: *u32,
) !void {
    if (fields.items.len == 0) return;
    const remove = try alloc.alloc(bool, fields.items.len);
    defer alloc.free(remove);
    @memset(remove, false);

    for (fields.items, 0..) |field, i| {
        for (claims) |claim| {
            if (!std.mem.eql(u8, field.path, claim.path)) continue;
            const claimed_scalar = claim.bounded_scalar_type orelse {
                remove[i] = true;
                break;
            };
            if (!std.mem.eql(u8, field.scalar_type, claimed_scalar)) {
                remove[i] = true;
                break;
            }
        }
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
    static_path_claims: *std.ArrayListUnmanaged(StaticPathClaim),
    skipped_dynamic_fields: *u32,
    skipped_complex_fields: *u32,
    skipped_unbounded_fields: *u32,
) !void {
    const explicitly_unindexed = property.antfly_index != null and !property.antfly_index.?;
    const composite = property.any_of.len > 0 or property.one_of.len > 0 or property.all_of.len > 0 or property.not_schema != null or property.if_schema != null;
    const bounded_scalar = if (explicitly_unindexed or
        property.item != null or
        property.properties.len > 0 or
        composite)
        null
    else
        scalarType(property);
    const claim_path = try alloc.dupe(u8, path);
    errdefer alloc.free(claim_path);
    try static_path_claims.append(alloc, .{
        .path = claim_path,
        .bounded_scalar_type = bounded_scalar,
    });
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
                static_path_claims,
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
