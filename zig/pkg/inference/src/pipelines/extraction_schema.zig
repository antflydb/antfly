// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical extraction schema compiler. Owns every string, slice and AST node;
//! no parsed JSON or request buffer must outlive CompiledSchema. This module
//! does not publish model capabilities or execute inference.
const std = @import("std");
pub const constraints = @import("extraction_constraints.zig");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
const Object = std.json.ObjectMap;

pub const DType = enum { str, list };
pub const Cardinality = enum { optional_one, required_one, zero_or_more, one_or_more };
pub const RecordMode = enum { natural, latent, anchorless };
pub const OccurrencePolicy = enum { all, first, error_on_ambiguous, latent_all };
pub const ClassificationMode = enum { single, multi, ordinal };
pub const Activation = enum { auto, sigmoid, softmax };
pub const RegexValidator = struct {
    pattern: []const u8,
    mode: enum { full, partial } = .full,
    exclude: bool = false,
    /// Python re flags, checked by the executable regex compiler callback.
    flags: u32 = 2,
};
pub const Entity = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    dtype: DType = .list,
    threshold: ?f64 = null,
    validators: []const RegexValidator = &.{},
};
pub const AttributeGroup = struct {
    name: []const u8,
    labels: []const []const u8,
    prompt_labels: []const []const u8,
    multi_label: bool = false,
    threshold: f64 = 0.5,
    /// Null applies to all entities; an explicitly empty list applies to none.
    applies_to: ?[]const usize = null,
    qualify_labels: bool = false,
};
pub const LabelDefinition = struct { name: []const u8, description: ?[]const u8 = null };
pub const Example = struct { input: []const u8, label: []const u8 };
pub const Classification = struct {
    task: constraints.Task,
    mode: ClassificationMode,
    /// Explicit set/ordinal selection semantics. Ordinary single/multi requests
    /// retain extraction's activation and best-label fallback; a constraint
    /// program also enables structured selection for the complete request.
    structured_selection: bool = false,
    activation: Activation = .auto,
    label_definitions: []const LabelDefinition,
    prompt: ?[]const u8 = null,
    examples: []const Example = &.{},
    /// Legacy presentation option, separate from constrained set cardinality.
    top_k: usize = 1,
    hypothesis_template: ?[]const u8 = null,
};

/// The constraint solver selects the complete classification collection. A
/// structured sibling therefore changes how every classification is selected.
pub fn usesStructuredClassification(classifications: []const Classification, has_constraints: bool) bool {
    if (has_constraints) return true;
    for (classifications) |classification| if (classification.structured_selection) return true;
    return false;
}

pub const Field = struct {
    name: []const u8,
    dtype: DType = .str,
    description: ?[]const u8 = null,
    threshold: ?f64 = null,
    choices: []const []const u8 = &.{},
    cardinality: ?Cardinality = null,
    exclusive: bool = false,
    validators: []const RegexValidator = &.{},
};
pub const Structure = struct {
    name: []const u8,
    fields: []const Field,
    /// Null intentionally preserves upstream's one-record legacy decoder.
    mode: ?RecordMode = null,
    anchor: ?usize = null,
    occurrence_policy: ?OccurrencePolicy = null,
};
pub const Relation = struct {
    name: []const u8,
    source: ?[]const u8 = null,
    target: ?[]const u8 = null,
    description: ?[]const u8 = null,
    threshold: ?f64 = null,
};
pub const JointEntity = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    threshold: ?f64 = null,
    candidate_threshold: ?f64 = null,
    max_candidates: ?usize = null,
    allow_nested: ?bool = null,
};
pub const JointRelation = struct {
    name: []const u8,
    head: []const usize,
    tail: []const usize,
    description: ?[]const u8 = null,
    threshold: ?f64 = null,
    candidate_threshold: ?f64 = null,
    directed: bool = true,
    symmetric: bool = false,
    inverse: ?usize = null,
    allow_self: bool = false,
    max_per_head: ?usize = null,
    max_per_tail: ?usize = null,
};
pub const JointConstraint = union(enum) {
    typed_endpoints: struct { relation: ?usize, head: []const usize, tail: []const usize },
    no_self_loops: ?usize,
    unique_pair: struct { relation: ?usize, directed: bool },
    unique_slot: struct { relation: ?usize, slot: enum { head, tail, slot } },
    entity_overlap: enum { allow, disallow, nested },
    max_per_head: struct { relation: ?usize, limit: usize },
    max_per_tail: struct { relation: ?usize, limit: usize },
    symmetric: usize,
    inverse: struct { relation: usize, inverse: usize },
    acyclic: usize,

    pub fn jsonStringify(self: JointConstraint, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("operator");
        try writer.write(@tagName(self));
        try writer.objectField("arguments");
        switch (self) {
            inline else => |arguments| try writer.write(arguments),
        }
        try writer.endObject();
    }
};
pub const JointSchema = struct {
    entities: []const JointEntity,
    relations: []const JointRelation,
    constraints: []const JointConstraint,
};
pub const Schema = struct {
    source_version: u32,
    entities: []const Entity = &.{},
    entity_attributes: []const AttributeGroup = &.{},
    classifications: []const Classification = &.{},
    structures: []const Structure = &.{},
    relations: []const Relation = &.{},
    classification_constraints: constraints.Program,
    joint_ie: ?JointSchema = null,
};
pub const Limits = struct {
    max_schema_bytes: usize = 64 * 1024,
    max_json_depth: usize = 32,
    max_tasks: usize = 32,
    max_labels: usize = 128,
    max_total_labels: usize = 512,
    max_fields: usize = 256,
    max_attribute_groups: usize = 32,
    max_examples: usize = 128,
    max_validators: usize = 64,
    max_regex_bytes: usize = 4096,
    max_constraint_nodes: usize = 512,
};
pub const Options = struct {
    schema_version: u32 = 2,
    limits: Limits = .{},
    regex_context: ?*anyopaque = null,
    /// Must compile syntax/flags using the same bounded engine used at execution.
    /// Called before a schema with validators can be accepted.
    validate_regex_fn: ?*const fn (?*anyopaque, RegexValidator) anyerror!void = null,
};
pub const CompiledSchema = struct {
    arena: std.heap.ArenaAllocator,
    schema: Schema,
    fingerprint: [32]u8,
    pub fn deinit(self: *CompiledSchema) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const SharedItem = struct { schema_json: []const u8, options_json: []const u8 = "{}" };
pub const ItemOverride = struct { schema_json: ?[]const u8 = null, options_json: ?[]const u8 = null };
/// Borrowed complete replacements. An explicit empty object never merges with
/// shared fields, and remains subject to normal schema validation by compile.
pub fn effectiveItem(shared: SharedItem, override: ItemOverride) SharedItem {
    return .{ .schema_json = override.schema_json orelse shared.schema_json, .options_json = override.options_json orelse shared.options_json };
}

pub fn compile(allocator: Allocator, json: []const u8, options: Options) !CompiledSchema {
    if (options.schema_version != 2) return error.UnsupportedExtractionSchemaVersion;
    return compileInternal(allocator, json, options);
}

/// Explicit migration adapter for existing canonical schemas. It normalizes
/// string/list aliases but preserves the original single-operation restriction.
pub fn compileLegacy(allocator: Allocator, json: []const u8, options: Options) !CompiledSchema {
    var legacy = options;
    legacy.schema_version = 1;
    return compileInternal(allocator, json, legacy);
}

fn compileInternal(allocator: Allocator, json: []const u8, options: Options) !CompiledSchema {
    const limits = options.limits;
    if (limits.max_tasks == 0 or limits.max_tasks > constraints.max_tasks or limits.max_labels == 0 or limits.max_labels > constraints.max_labels or
        limits.max_json_depth == 0 or limits.max_json_depth > 64 or limits.max_constraint_nodes == 0 or limits.max_constraint_nodes > 4096)
        return error.InvalidExtractionSchemaLimits;
    if (json.len > limits.max_schema_bytes) return error.ExtractionSchemaLimitExceeded;
    try preflightDepth(json, limits.max_json_depth);
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const alloc = arena.allocator();
    const parsed = try std.json.parseFromSlice(Value, alloc, json, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    var parser = Parser{ .allocator = alloc, .options = options };
    const schema = try parser.schema(parsed.value);
    // Struct fields and normalized metadata have stable ordering. Declaration
    // order of tasks/labels/record fields is intentionally part of identity.
    const canonical = try std.json.Stringify.valueAlloc(alloc, schema, .{});
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-extraction-schema-ir/v2\x00");
    hash.update(canonical);
    var fingerprint: [32]u8 = undefined;
    hash.final(&fingerprint);
    return .{ .arena = arena, .schema = schema, .fingerprint = fingerprint };
}

const Parser = struct {
    allocator: Allocator,
    options: Options,
    total_labels: usize = 0,
    total_fields: usize = 0,
    total_examples: usize = 0,
    total_validators: usize = 0,
    // Presence is needed until the complete collection's route is known. It is
    // not semantic IR: accepted ordinary omitted/explicit 1 remain identical.
    has_explicit_classification_top_k: bool = false,

    fn schema(self: *Parser, value: Value) !Schema {
        const obj = try object(value);
        try keys(obj, &.{ "entities", "entity_definitions", "entity_attributes", "classifications", "structures", "relations", "classification_constraints", "joint_ie" });
        if (self.options.schema_version == 1) {
            inline for (.{ "entity_definitions", "entity_attributes", "classification_constraints", "joint_ie" }) |name|
                if (present(obj, name) != null) return error.AdvancedExtractionSchemaRequiresVersion2;
        }
        const entities = if (present(obj, "entities")) |raw| try self.parseEntities(raw, present(obj, "entity_definitions")) else blk: {
            if (present(obj, "entity_definitions") != null) return error.EntityDefinitionsRequireEntities;
            break :blk &.{};
        };
        const attrs = if (present(obj, "entity_attributes")) |raw| try self.attributes(raw, entities) else &.{};
        const cls = if (present(obj, "classifications")) |raw| try self.classifications(raw) else &.{};
        const structures = if (present(obj, "structures")) |raw| try self.parseStructures(raw) else &.{};
        const relations = if (present(obj, "relations")) |raw| try self.parseRelations(raw) else &.{};
        const joint = if (present(obj, "joint_ie")) |raw| try self.parseJoint(raw) else null;
        const count = @as(usize, @intFromBool(entities.len > 0 or relations.len > 0)) + @as(usize, @intFromBool(cls.len > 0)) + @as(usize, @intFromBool(structures.len > 0));
        if (joint != null and count != 0) return error.MixedJointExtractionSchema;
        if (joint == null and count == 0) return error.EmptyExtractionSchema;
        if (self.options.schema_version == 1 and count > 1) return error.MixedExtractionOperations;
        if (cls.len + structures.len + relations.len + @as(usize, @intFromBool(entities.len > 0)) > self.options.limits.max_tasks)
            return error.ExtractionSchemaLimitExceeded;
        const tasks = try self.allocator.alloc(constraints.Task, cls.len);
        for (cls, tasks) |classification, *task| task.* = classification.task;
        const raw_constraints = present(obj, "classification_constraints") orelse Value{ .array = std.array_list.Managed(Value).init(self.allocator) };
        if (raw_constraints != .array) return error.InvalidExtractionSchema;
        if (raw_constraints.array.items.len > 0) {
            if (cls.len == 0 or joint != null) return error.ClassificationConstraintsRequireTasks;
        }
        if (self.has_explicit_classification_top_k and usesStructuredClassification(cls, raw_constraints.array.items.len > 0))
            return error.ConstrainedClassificationTopKUnsupported;
        const program = try constraints.compileValue(self.allocator, tasks, raw_constraints, .{ .max_nodes = self.options.limits.max_constraint_nodes, .max_depth = self.options.limits.max_json_depth });
        return .{ .source_version = self.options.schema_version, .entities = entities, .entity_attributes = attrs, .classifications = cls, .structures = structures, .relations = relations, .classification_constraints = program, .joint_ie = joint };
    }

    fn labels(self: *Parser, value: Value, allow_empty: bool) ![]const []const u8 {
        const values = try array(value);
        if ((!allow_empty and values.len == 0) or values.len > self.options.limits.max_labels) return error.InvalidExtractionLabels;
        try self.charge(&self.total_labels, values.len, self.options.limits.max_total_labels);
        const out = try self.allocator.alloc([]const u8, values.len);
        for (values, out, 0..) |raw, *name, i| {
            name.* = try clean(raw, false);
            try unique(out[0..i], name.*);
        }
        return out;
    }
    fn charge(_: *Parser, counter: *usize, amount: usize, limit: usize) !void {
        counter.* = std.math.add(usize, counter.*, amount) catch return error.ExtractionSchemaLimitExceeded;
        if (counter.* > limit) return error.ExtractionSchemaLimitExceeded;
    }
    fn validators(self: *Parser, value: ?Value) ![]const RegexValidator {
        const raw = value orelse return &.{};
        const values = try array(raw);
        try self.charge(&self.total_validators, values.len, self.options.limits.max_validators);
        if (values.len == 0) return &.{};
        const validate = self.options.validate_regex_fn orelse return error.RegexValidationUnavailable;
        const out = try self.allocator.alloc(RegexValidator, values.len);
        for (values, out) |entry, *validator| {
            const obj = try object(entry);
            try keys(obj, &.{ "type", "pattern", "mode", "exclude", "flags" });
            if (present(obj, "type")) |kind| if (!std.mem.eql(u8, try text(kind), "regex")) return error.UnsupportedExtractionValidator;
            const pattern = try text(try required(obj, "pattern"));
            if (pattern.len > self.options.limits.max_regex_bytes or !std.unicode.utf8ValidateSlice(pattern)) return error.InvalidExtractionRegex;
            validator.* = .{
                .pattern = pattern,
                .mode = try enumeration(@FieldType(RegexValidator, "mode"), obj, "mode", .full),
                .exclude = try boolean(obj, "exclude", false),
                .flags = if (present(obj, "flags")) |flags| std.math.cast(u32, try number(flags)) orelse return error.InvalidExtractionRegex else 2,
            };
            try validate(self.options.regex_context, validator.*);
        }
        return out;
    }
    fn parseEntities(self: *Parser, value: Value, definitions: ?Value) ![]const Entity {
        const names = try self.labels(value, true);
        const out = try self.allocator.alloc(Entity, names.len);
        const defs = if (definitions) |raw| try object(raw) else null;
        if (defs) |map| for (map.keys()) |name| {
            _ = try indexOf(names, name);
        };
        for (names, out) |name, *entity| {
            entity.* = .{ .name = name };
            if (defs) |map| if (map.get(name)) |definition| {
                const obj = try object(definition);
                try keys(obj, &.{ "description", "dtype", "type", "threshold", "validators" });
                entity.description = try optionalText(obj, "description", false);
                entity.dtype = try dtype(obj, .list);
                entity.threshold = try probability(obj, "threshold");
                entity.validators = try self.validators(present(obj, "validators"));
            };
        }
        return out;
    }
    fn attributes(self: *Parser, value: Value, entities_: []const Entity) ![]const AttributeGroup {
        const obj = try object(value);
        if (entities_.len == 0) return error.EntityAttributesRequireEntities;
        if (obj.count() > self.options.limits.max_attribute_groups) return error.ExtractionSchemaLimitExceeded;
        const entity_names = try namesOf(self.allocator, entities_);
        const out = try self.allocator.alloc(AttributeGroup, obj.count());
        for (obj.keys(), obj.values(), out, 0..) |name, raw, *group, i| {
            _ = try clean(.{ .string = name }, false);
            for ([_][]const u8{ "text", "confidence", "start", "end" }) |reserved| if (std.mem.eql(u8, name, reserved)) return error.ReservedAttributeGroup;
            const config = try object(raw);
            try keys(config, &.{ "labels", "multi_label", "threshold", "applies_to", "qualify_labels" });
            const values = try self.labels(try required(config, "labels"), false);
            const qualified = try boolean(config, "qualify_labels", false);
            const prompts = try self.allocator.alloc([]const u8, values.len);
            for (values, prompts, 0..) |label, *prompt, label_index| {
                for (out[0..i]) |prior| try unique(prior.labels, label);
                prompt.* = if (qualified) try std.fmt.allocPrint(self.allocator, "{s}: {s}", .{ name, label }) else label;
                try unique(entity_names, prompt.*);
                try unique(prompts[0..label_index], prompt.*);
                for (out[0..i]) |prior| try unique(prior.prompt_labels, prompt.*);
            }
            group.* = .{
                .name = name,
                .labels = values,
                .prompt_labels = prompts,
                .multi_label = try boolean(config, "multi_label", false),
                .threshold = (try probability(config, "threshold")) orelse 0.5,
                .qualify_labels = qualified,
                .applies_to = if (present(config, "applies_to")) |raw_types| try self.references(raw_types, entity_names, true) else null,
            };
        }
        return out;
    }
    fn classifications(self: *Parser, value: Value) ![]const Classification {
        const values = try array(value);
        if (values.len > self.options.limits.max_tasks) return error.ExtractionSchemaLimitExceeded;
        const out = try self.allocator.alloc(Classification, values.len);
        for (values, out, 0..) |raw, *classification, i| {
            const obj = try object(raw);
            try keys(obj, &.{ "name", "labels", "label_definitions", "multi_label", "top_k", "hypothesis_template", "mode", "min_labels", "max_labels", "ordered", "threshold", "candidate_threshold", "activation", "temperature", "default", "prompt", "instruction", "examples" });
            const name = try clean(try required(obj, "name"), true);
            for (out[0..i]) |prior| if (std.mem.eql(u8, name, prior.task.name)) return error.DuplicateClassificationTask;
            const names = try self.labels(try required(obj, "labels"), false);
            for (names) |label| _ = try clean(.{ .string = label }, true);
            const multi = try boolean(obj, "multi_label", false);
            const mode = try enumeration(ClassificationMode, obj, "mode", if (multi) .multi else .single);
            if (present(obj, "mode") != null and present(obj, "multi_label") != null and multi != (mode == .multi)) return error.ConflictingClassificationMode;
            const defs = try self.allocator.alloc(LabelDefinition, names.len);
            const metadata = if (present(obj, "label_definitions")) |v| try object(v) else null;
            if (metadata) |map| for (map.keys()) |key| {
                _ = try indexOf(names, key);
            };
            for (names, defs) |label, *definition| {
                definition.* = .{ .name = label };
                if (metadata) |map| if (map.get(label)) |raw_meta| {
                    const meta = try object(raw_meta);
                    try keys(meta, &.{"description"});
                    definition.description = try optionalText(meta, "description", true);
                };
            }
            if (present(obj, "prompt") != null and present(obj, "instruction") != null) return error.ConflictingClassificationPrompt;
            const prompt = (try optionalText(obj, "prompt", true)) orelse (try optionalText(obj, "instruction", true));
            const default_label = if (present(obj, "default")) |v| try indexOf(names, try text(v)) else null;
            var minimum = (try optionalNumber(obj, "min_labels")) orelse if (mode == .multi) @as(usize, 0) else @as(usize, 1);
            if (default_label != null) minimum = @max(minimum, 1);
            const maximum = if (obj.contains("max_labels")) try optionalNumber(obj, "max_labels") else if (mode == .multi) null else @as(?usize, 1);
            const top_k = (try optionalNumber(obj, "top_k")) orelse 1;
            if (top_k == 0) return error.InvalidClassificationTopK;
            self.has_explicit_classification_top_k = self.has_explicit_classification_top_k or obj.contains("top_k");
            const task = constraints.Task{
                .name = name,
                .labels = names,
                .min_labels = minimum,
                .max_labels = maximum,
                .ordered = if (mode == .ordinal) true else try boolean(obj, "ordered", false),
                .threshold = (try probability(obj, "threshold")) orelse 0.5,
                .candidate_threshold = try probability(obj, "candidate_threshold"),
                .temperature = (try optionalFloat(obj, "temperature")) orelse 1,
                .default_label = default_label,
            };
            if (mode == .ordinal and present(obj, "ordered") != null and !try boolean(obj, "ordered", true)) return error.ConflictingClassificationMode;
            try constraints.validateTask(task);
            classification.* = .{
                .task = task,
                .mode = mode,
                .structured_selection = mode == .ordinal or obj.contains("min_labels") or obj.contains("max_labels") or obj.contains("default") or obj.contains("candidate_threshold") or obj.contains("ordered"),
                .activation = try enumeration(Activation, obj, "activation", .auto),
                .label_definitions = defs,
                .prompt = prompt,
                .examples = try self.examples(present(obj, "examples"), names),
                .top_k = top_k,
                .hypothesis_template = try optionalText(obj, "hypothesis_template", false),
            };
        }
        return out;
    }
    fn examples(self: *Parser, value: ?Value, labels_: []const []const u8) ![]const Example {
        const raw = value orelse return &.{};
        const values = try array(raw);
        try self.charge(&self.total_examples, values.len, self.options.limits.max_examples);
        const out = try self.allocator.alloc(Example, values.len);
        for (values, out) |entry, *example| {
            if (entry == .array) {
                if (entry.array.items.len != 2) return error.InvalidClassificationExample;
                example.* = .{ .input = try clean(entry.array.items[0], true), .label = try clean(entry.array.items[1], true) };
            } else {
                const obj = try object(entry);
                try keys(obj, &.{ "input", "label" });
                example.* = .{ .input = try clean(try required(obj, "input"), true), .label = try clean(try required(obj, "label"), true) };
            }
            _ = try indexOf(labels_, example.label);
        }
        return out;
    }
    fn parseStructures(self: *Parser, value: Value) ![]const Structure {
        const obj = try object(value);
        if (obj.count() > self.options.limits.max_tasks) return error.ExtractionSchemaLimitExceeded;
        const out = try self.allocator.alloc(Structure, obj.count());
        for (obj.keys(), obj.values(), out) |name, raw, *structure| {
            _ = try clean(.{ .string = name }, false);
            const config = try object(raw);
            try keys(config, &.{ "fields", "mode", "anchor", "occurrence_policy" });
            const fields_map = try object(try required(config, "fields"));
            if (fields_map.count() == 0) return error.EmptyStructureFields;
            try self.charge(&self.total_fields, fields_map.count(), self.options.limits.max_fields);
            try self.charge(&self.total_labels, fields_map.count(), self.options.limits.max_total_labels);
            const fields = try self.allocator.alloc(Field, fields_map.count());
            for (fields_map.keys(), fields_map.values(), fields) |field_name, field_raw, *field| field.* = try self.parseField(field_name, field_raw);
            const mode = try optionalEnum(RecordMode, config, "mode");
            var anchor: ?usize = null;
            if (present(config, "anchor")) |v| {
                if (mode != .natural) return error.InvalidStructureAnchor;
                anchor = try indexOf(try namesOf(self.allocator, fields), try text(v));
            } else if (mode == .natural) anchor = 0;
            structure.* = .{ .name = name, .fields = fields, .mode = mode, .anchor = anchor, .occurrence_policy = try optionalEnum(OccurrencePolicy, config, "occurrence_policy") };
        }
        return out;
    }
    fn parseField(self: *Parser, name: []const u8, value: Value) !Field {
        _ = try clean(.{ .string = name }, false);
        if (value == .string) return .{ .name = name, .dtype = try dtypeName(value.string) };
        const obj = try object(value);
        try keys(obj, &.{ "type", "dtype", "description", "threshold", "enum", "choices", "cardinality", "exclusive", "validators" });
        if (present(obj, "enum") != null and present(obj, "choices") != null) return error.ConflictingStructureChoices;
        const choices = if (present(obj, "enum") orelse present(obj, "choices")) |raw| try self.labels(raw, false) else &.{};
        const field_dtype = try dtype(obj, .str);
        const cardinality = try optionalEnum(Cardinality, obj, "cardinality");
        if (cardinality) |card| {
            const list = card == .zero_or_more or card == .one_or_more;
            if (list != (field_dtype == .list)) return error.ConflictingFieldCardinality;
        }
        return .{ .name = name, .dtype = field_dtype, .description = try optionalText(obj, "description", false), .threshold = try probability(obj, "threshold"), .choices = choices, .cardinality = cardinality, .exclusive = try boolean(obj, "exclusive", false), .validators = try self.validators(present(obj, "validators")) };
    }
    fn parseRelations(self: *Parser, value: Value) ![]const Relation {
        const values = try array(value);
        if (values.len > self.options.limits.max_tasks) return error.ExtractionSchemaLimitExceeded;
        const out = try self.allocator.alloc(Relation, values.len);
        for (values, out, 0..) |entry, *relation, i| {
            const obj = try object(entry);
            try keys(obj, &.{ "type", "source", "target", "description", "threshold" });
            const name = try clean(try required(obj, "type"), false);
            const source = try optionalText(obj, "source", false);
            const target = try optionalText(obj, "target", false);
            if (source == null and target != null) return error.InvalidRelationEndpoints;
            for (out[0..i]) |prior| if (std.mem.eql(u8, name, prior.name) and optionalEqual(source, prior.source) and optionalEqual(target, prior.target)) return error.DuplicateRelation;
            relation.* = .{ .name = name, .source = source, .target = target, .description = try optionalText(obj, "description", false), .threshold = try probability(obj, "threshold") };
        }
        return out;
    }
    fn references(self: *Parser, raw: Value, names: []const []const u8, allow_empty: bool) ![]const usize {
        const values = if (raw == .string) try self.allocator.dupe([]const u8, &.{try clean(raw, false)}) else try self.labels(raw, allow_empty);
        if (!allow_empty and values.len == 0) return error.InvalidExtractionReference;
        const out = try self.allocator.alloc(usize, values.len);
        for (values, out) |value, *index| index.* = try indexOf(names, value);
        return out;
    }
    fn parseJoint(self: *Parser, value: Value) !JointSchema {
        const obj = try object(value);
        try keys(obj, &.{ "entities", "relations", "constraints" });
        const entity_map = try object(try required(obj, "entities"));
        const relation_map = if (present(obj, "relations")) |raw| try object(raw) else Object{};
        if (entity_map.count() == 0 or entity_map.count() > self.options.limits.max_labels or relation_map.count() > self.options.limits.max_tasks) return error.InvalidJointSchema;
        try self.charge(&self.total_labels, entity_map.count() + relation_map.count(), self.options.limits.max_total_labels);
        const entities_ = try self.allocator.alloc(JointEntity, entity_map.count());
        for (entity_map.keys(), entity_map.values(), entities_) |name, raw, *entity| {
            _ = try clean(.{ .string = name }, false);
            if (raw == .string) {
                entity.* = .{ .name = name, .description = try clean(raw, false) };
                continue;
            }
            const config = try object(raw);
            try keys(config, &.{ "description", "threshold", "candidate_threshold", "max_candidates", "allow_nested" });
            const maximum = try optionalNumber(config, "max_candidates");
            if (maximum != null and maximum.? == 0) return error.InvalidJointCandidateLimit;
            entity.* = .{ .name = name, .description = try optionalText(config, "description", false), .threshold = try probability(config, "threshold"), .candidate_threshold = try probability(config, "candidate_threshold"), .max_candidates = maximum, .allow_nested = try optionalBool(config, "allow_nested") };
            if (entity.threshold) |threshold| if (threshold == 0 or threshold == 1) return error.InvalidJointDecisionThreshold;
        }
        const names = try namesOf(self.allocator, entities_);
        const relations_ = try self.allocator.alloc(JointRelation, relation_map.count());
        for (relation_map.keys(), relation_map.values(), relations_) |name, raw, *relation| {
            _ = try clean(.{ .string = name }, false);
            const config = try object(raw);
            try keys(config, &.{ "head", "tail", "description", "threshold", "candidate_threshold", "directed", "symmetric", "inverse", "allow_self", "max_per_head", "max_per_tail" });
            const head = try self.references(try required(config, "head"), names, false);
            const tail = try self.references(try required(config, "tail"), names, false);
            const symmetric = try boolean(config, "symmetric", false);
            const inverse = if (present(config, "inverse")) |v| try indexOf(relation_map.keys(), try text(v)) else null;
            if (symmetric and (inverse != null or !sameSet(head, tail))) return error.InvalidSymmetricRelation;
            relation.* = .{ .name = name, .head = head, .tail = tail, .description = try optionalText(config, "description", false), .threshold = try probability(config, "threshold"), .candidate_threshold = try probability(config, "candidate_threshold"), .directed = if (symmetric) false else try boolean(config, "directed", true), .symmetric = symmetric, .inverse = inverse, .allow_self = try boolean(config, "allow_self", false), .max_per_head = try optionalNumber(config, "max_per_head"), .max_per_tail = try optionalNumber(config, "max_per_tail") };
            if (relation.threshold) |threshold| if (threshold == 0 or threshold == 1) return error.InvalidJointDecisionThreshold;
        }
        for (relations_, 0..) |relation, i| if (relation.inverse) |inverse| {
            const other = relations_[inverse];
            if (inverse == i or !sameSet(relation.head, other.tail) or !sameSet(relation.tail, other.head) or (other.inverse != null and other.inverse.? != i)) return error.InvalidInverseRelation;
        };
        const raw_constraints = if (present(obj, "constraints")) |raw| try array(raw) else &.{};
        if (raw_constraints.len > self.options.limits.max_constraint_nodes) return error.ExtractionSchemaLimitExceeded;
        const joint_constraints = try self.allocator.alloc(JointConstraint, raw_constraints.len);
        for (raw_constraints, joint_constraints) |raw, *out| out.* = try self.jointConstraint(raw, names, relation_map.keys());
        return .{ .entities = entities_, .relations = relations_, .constraints = joint_constraints };
    }
    fn jointConstraint(self: *Parser, value: Value, entity_names: []const []const u8, relation_names: []const []const u8) !JointConstraint {
        const obj = try object(value);
        const kind = try text(try required(obj, "type"));
        const relation = if (present(obj, "relation")) |raw| try indexOf(relation_names, try text(raw)) else null;
        if (std.mem.eql(u8, kind, "TypedEndpoints")) {
            try keys(obj, &.{ "type", "relation", "head_types", "tail_types" });
            return .{ .typed_endpoints = .{ .relation = relation, .head = if (present(obj, "head_types")) |raw| try self.references(raw, entity_names, true) else &.{}, .tail = if (present(obj, "tail_types")) |raw| try self.references(raw, entity_names, true) else &.{} } };
        }
        if (std.mem.eql(u8, kind, "NoSelfLoops")) {
            try keys(obj, &.{ "type", "relation" });
            return .{ .no_self_loops = relation };
        }
        if (std.mem.eql(u8, kind, "UniqueRelationPair")) {
            try keys(obj, &.{ "type", "relation", "directed" });
            return .{ .unique_pair = .{ .relation = relation, .directed = try boolean(obj, "directed", true) } };
        }
        if (std.mem.eql(u8, kind, "UniqueRelationSlot")) {
            try keys(obj, &.{ "type", "relation", "slot" });
            return .{ .unique_slot = .{ .relation = relation, .slot = try enumeration(@FieldType(@FieldType(JointConstraint, "unique_slot"), "slot"), obj, "slot", .head) } };
        }
        if (std.mem.eql(u8, kind, "EntityOverlapPolicy")) {
            try keys(obj, &.{ "type", "policy" });
            return .{ .entity_overlap = try enumeration(@FieldType(JointConstraint, "entity_overlap"), obj, "policy", .disallow) };
        }
        inline for (.{ .{ "MaxRelationsPerHead", "max_per_head" }, .{ "MaxRelationsPerTail", "max_per_tail" } }) |entry| if (std.mem.eql(u8, kind, entry[0])) {
            try keys(obj, &.{ "type", "relation", "limit" });
            return @unionInit(JointConstraint, entry[1], .{ .relation = relation, .limit = try number(try required(obj, "limit")) });
        };
        if (std.mem.eql(u8, kind, "SymmetricRelation")) {
            try keys(obj, &.{ "type", "relation" });
            return .{ .symmetric = relation orelse return error.InvalidJointConstraint };
        }
        if (std.mem.eql(u8, kind, "AcyclicRelation")) {
            try keys(obj, &.{ "type", "relation" });
            return .{ .acyclic = relation orelse return error.InvalidJointConstraint };
        }
        if (std.mem.eql(u8, kind, "InverseRelation")) {
            try keys(obj, &.{ "type", "relation", "inverse" });
            const inverse = try indexOf(relation_names, try text(try required(obj, "inverse")));
            if (relation == null or inverse == relation.?) return error.InvalidJointConstraint;
            return .{ .inverse = .{ .relation = relation.?, .inverse = inverse } };
        }
        return error.UnknownJointConstraintType;
    }
};

fn object(value: Value) !Object {
    return if (value == .object) value.object else error.InvalidExtractionSchema;
}
fn array(value: Value) ![]const Value {
    return if (value == .array) value.array.items else error.InvalidExtractionSchema;
}
fn text(value: Value) ![]const u8 {
    return if (value == .string) value.string else error.InvalidExtractionSchema;
}
fn required(obj: Object, name: []const u8) !Value {
    return present(obj, name) orelse error.MissingExtractionSchemaField;
}
fn present(obj: Object, name: []const u8) ?Value {
    const value = obj.get(name) orelse return null;
    return if (value == .null) null else value;
}
fn number(value: Value) !usize {
    if (value != .integer or value.integer < 0) return error.InvalidExtractionNumber;
    return std.math.cast(usize, value.integer) orelse error.InvalidExtractionNumber;
}
fn optionalNumber(obj: Object, name: []const u8) !?usize {
    return if (present(obj, name)) |value| try number(value) else null;
}
fn optionalBool(obj: Object, name: []const u8) !?bool {
    const value = present(obj, name) orelse return null;
    return if (value == .bool) value.bool else error.InvalidExtractionBoolean;
}
fn boolean(obj: Object, name: []const u8, default: bool) !bool {
    return (try optionalBool(obj, name)) orelse default;
}
fn optionalFloat(obj: Object, name: []const u8) !?f64 {
    const value = present(obj, name) orelse return null;
    const result: f64 = switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        else => return error.InvalidExtractionNumber,
    };
    if (!std.math.isFinite(result)) return error.InvalidExtractionNumber;
    return result;
}
fn probability(obj: Object, name: []const u8) !?f64 {
    const value = (try optionalFloat(obj, name)) orelse return null;
    if (value < 0 or value > 1) return error.InvalidExtractionThreshold;
    return value;
}
fn optionalText(obj: Object, name: []const u8, classification: bool) !?[]const u8 {
    return if (present(obj, name)) |value| try clean(value, classification) else null;
}
fn clean(value: Value, classification: bool) ![]const u8 {
    const str = try text(value);
    if (std.mem.trim(u8, str, " \t\r\n").len == 0 or !std.unicode.utf8ValidateSlice(str)) return error.InvalidExtractionName;
    for ([_][]const u8{ "[P]", "[L]", "[C]", "[E]", "[R]", "[DESCRIPTION]", "[EXAMPLE]", "[OUTPUT]", "[SEP_TEXT]", "[SEP_STRUCT]" }) |marker|
        if (std.mem.indexOf(u8, str, marker) != null) return error.ReservedExtractionMarker;
    if (classification and (std.mem.indexOfScalar(u8, str, '(') != null or std.mem.indexOfScalar(u8, str, ')') != null)) return error.ReservedExtractionMarker;
    return str;
}
fn unique(names: []const []const u8, name: []const u8) !void {
    for (names) |old| if (std.mem.eql(u8, name, old)) return error.DuplicateExtractionLabel;
}
fn indexOf(names: []const []const u8, name: []const u8) !usize {
    for (names, 0..) |old, i| if (std.mem.eql(u8, name, old)) return i;
    return error.UnknownExtractionReference;
}
fn namesOf(allocator: Allocator, values: anytype) ![]const []const u8 {
    const names = try allocator.alloc([]const u8, values.len);
    for (values, names) |value, *name| name.* = value.name;
    return names;
}
fn optionalEqual(a: ?[]const u8, b: ?[]const u8) bool {
    return if (a) |left| if (b) |right| std.mem.eql(u8, left, right) else false else b == null;
}
fn sameSet(a: []const usize, b: []const usize) bool {
    if (a.len != b.len) return false;
    for (a) |left| {
        if (std.mem.indexOfScalar(usize, b, left) == null) return false;
    }
    return true;
}
fn dtypeName(name: []const u8) !DType {
    if (std.mem.eql(u8, name, "str") or std.mem.eql(u8, name, "string")) return .str;
    if (std.mem.eql(u8, name, "list") or std.mem.eql(u8, name, "array")) return .list;
    return error.UnsupportedExtractionFieldType;
}
fn dtype(obj: Object, default: DType) !DType {
    const first = if (present(obj, "type")) |value| try dtypeName(try text(value)) else null;
    const second = if (present(obj, "dtype")) |value| try dtypeName(try text(value)) else null;
    if (first != null and second != null and first.? != second.?) return error.ConflictingExtractionFieldType;
    return first orelse second orelse default;
}
fn optionalEnum(comptime T: type, obj: Object, name: []const u8) !?T {
    const raw = present(obj, name) orelse return null;
    return std.meta.stringToEnum(T, try text(raw)) orelse error.InvalidExtractionEnum;
}
fn enumeration(comptime T: type, obj: Object, name: []const u8, default: T) !T {
    return (try optionalEnum(T, obj, name)) orelse default;
}
fn keys(obj: Object, allowed: []const []const u8) !void {
    for (obj.keys()) |key| {
        var found = false;
        for (allowed) |name| if (std.mem.eql(u8, key, name)) {
            found = true;
            break;
        };
        if (!found) return error.UnknownExtractionSchemaField;
    }
}
fn preflightDepth(json: []const u8, limit: usize) !void {
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (json) |char| {
        if (quoted) {
            if (escaped) {
                escaped = false;
            } else if (char == '\\') {
                escaped = true;
            } else if (char == '"') {
                quoted = false;
            }
            continue;
        }
        if (char == '"') {
            quoted = true;
            continue;
        }
        if (char == '{' or char == '[') {
            depth += 1;
            if (depth > limit) return error.ExtractionSchemaLimitExceeded;
        } else if (char == '}' or char == ']') {
            if (depth == 0) return error.InvalidExtractionSchema;
            depth -= 1;
        }
    }
    if (depth != 0 or quoted) return error.InvalidExtractionSchema;
}

test "extraction v2 mixed schema owns data and normalizes metadata identity" {
    const json = "{\"entities\":[\"person\",\"company\"],\"entity_definitions\":{\"person\":{\"description\":\"a human\"}},\"entity_attributes\":{\"status\":{\"labels\":[\"active\",\"inactive\"],\"applies_to\":[\"person\"],\"qualify_labels\":true}},\"classifications\":[{\"name\":\"sentiment\",\"labels\":[\"negative\",\"positive\"],\"mode\":\"ordinal\",\"prompt\":\"Rate sentiment\",\"examples\":[[\"Great\",\"positive\"]]}],\"structures\":{\"employment\":{\"mode\":\"natural\",\"fields\":{\"name\":{\"type\":\"str\",\"cardinality\":\"required_one\"},\"skills\":{\"type\":\"list\",\"cardinality\":\"zero_or_more\"}}}},\"relations\":[{\"type\":\"works_for\",\"source\":\"person\",\"target\":\"company\"}]}";
    const buffer = try std.testing.allocator.dupe(u8, json);
    var compiled = try compile(std.testing.allocator, buffer, .{});
    defer compiled.deinit();
    @memset(buffer, 'x');
    std.testing.allocator.free(buffer);
    try std.testing.expectEqualStrings("person", compiled.schema.entities[0].name);
    try std.testing.expectEqualStrings("status: active", compiled.schema.entity_attributes[0].prompt_labels[0]);
    try std.testing.expectEqual(@as(?usize, 0), compiled.schema.structures[0].anchor);
    try std.testing.expect(compiled.schema.classifications[0].task.ordered);
    var first = try compile(std.testing.allocator, "{\"entities\":[\"a\",\"b\"],\"entity_definitions\":{\"a\":{\"threshold\":0.5},\"b\":{\"dtype\":\"list\"}}}", .{});
    defer first.deinit();
    var second = try compile(std.testing.allocator, "{\"entity_definitions\":{\"b\":{\"type\":\"array\"},\"a\":{\"threshold\":0.5}},\"entities\":[\"a\",\"b\"]}", .{});
    defer second.deinit();
    try std.testing.expectEqualSlices(u8, &first.fingerprint, &second.fingerprint);
}

test "extraction schema rejects silent semantic loss and preserves legacy adapter" {
    try std.testing.expectError(error.UnsupportedExtractionSchemaVersion, compile(std.testing.allocator, "{}", .{ .schema_version = 1 }));
    try std.testing.expectError(error.UnknownExtractionSchemaField, compile(std.testing.allocator, "{\"entities\":[\"person\"],\"instructions\":\"ignored\"}", .{}));
    try std.testing.expectError(error.ReservedExtractionMarker, compile(std.testing.allocator, "{\"classifications\":[{\"name\":\"topic\",\"labels\":[\"[L] injected\"]}]}", .{}));
    try std.testing.expectError(error.UnknownExtractionReference, compile(std.testing.allocator, "{\"entities\":[\"person\"],\"entity_definitions\":{\"other\":{}}}", .{}));
    try std.testing.expectError(error.DuplicateExtractionLabel, compile(std.testing.allocator, "{\"entities\":[\"person\"],\"entity_attributes\":{\"a\":{\"labels\":[\"x\"]},\"b\":{\"labels\":[\"x\"],\"qualify_labels\":true}}}", .{}));
    try std.testing.expectError(error.InvalidStructureAnchor, compile(std.testing.allocator, "{\"structures\":{\"s\":{\"mode\":\"latent\",\"anchor\":\"a\",\"fields\":{\"a\":\"str\"}}}}", .{}));
    try std.testing.expectError(error.RegexValidationUnavailable, compile(std.testing.allocator, "{\"entities\":[\"x\"],\"entity_definitions\":{\"x\":{\"validators\":[{\"pattern\":\"x+\"}]}}}", .{}));
    var legacy = try compileLegacy(std.testing.allocator, "{\"structures\":{\"s\":{\"fields\":{\"a\":\"string\"}}}}", .{});
    defer legacy.deinit();
    try std.testing.expectEqual(@as(u32, 1), legacy.schema.source_version);
    try std.testing.expectEqual(@as(?RecordMode, null), legacy.schema.structures[0].mode);
}

test "extraction classification preserves explicit structured selection intent" {
    const ordinary = [_][]const u8{
        "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"]}]}",
        "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"mode\":\"multi\",\"threshold\":0.7,\"activation\":\"sigmoid\",\"temperature\":2,\"prompt\":\"Choose\",\"examples\":[[\"Example\",\"a\"]]}]}",
    };
    for (ordinary) |json| {
        var compiled = try compile(std.testing.allocator, json, .{});
        defer compiled.deinit();
        try std.testing.expect(!compiled.schema.classifications[0].structured_selection);
    }
    const prefix = "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],";
    const suffix = "}]}";
    const structured = [_][]const u8{
        prefix ++ "\"mode\":\"ordinal\"" ++ suffix,
        prefix ++ "\"min_labels\":0" ++ suffix,
        prefix ++ "\"max_labels\":null" ++ suffix,
        prefix ++ "\"default\":\"b\"" ++ suffix,
        prefix ++ "\"candidate_threshold\":0.1" ++ suffix,
        prefix ++ "\"ordered\":false" ++ suffix,
    };
    for (structured) |json| {
        var compiled = try compile(std.testing.allocator, json, .{});
        defer compiled.deinit();
        try std.testing.expect(compiled.schema.classifications[0].structured_selection);
    }
}

test "extraction classification top_k rejects every structured route including explicit one" {
    const prefix = "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],";
    inline for (.{
        "\"mode\":\"ordinal\"",
        "\"min_labels\":0",
        "\"max_labels\":2",
        "\"max_labels\":null",
        "\"default\":\"a\"",
        "\"candidate_threshold\":0.1",
        "\"ordered\":false",
        "\"ordered\":true",
        "\"mode\":\"multi\",\"min_labels\":2,\"max_labels\":2",
    }) |advanced| {
        inline for (.{ "\"top_k\":1", "\"top_k\":2" }) |top_k| {
            try std.testing.expectError(error.ConstrainedClassificationTopKUnsupported, compile(std.testing.allocator, prefix ++ advanced ++ "," ++ top_k ++ "}]}", .{}));
        }
    }
    inline for (.{ "\"top_k\":1", "\"top_k\":2" }) |top_k| {
        try std.testing.expectError(error.ConstrainedClassificationTopKUnsupported, compile(std.testing.allocator, prefix ++ top_k ++ "}],\"classification_constraints\":[{\"type\":\"LabelRef\",\"task\":\"t\",\"label\":\"a\"}]}", .{}));
    }
}

test "extraction classification top_k rejects structured siblings in either order" {
    const structured = "{\"name\":\"structured\",\"labels\":[\"x\",\"y\"],\"min_labels\":1,\"max_labels\":1}";
    inline for (.{ "1", "2" }) |top_k| {
        const ordinary = "{\"name\":\"ordinary\",\"labels\":[\"a\",\"b\",\"c\"],\"top_k\":" ++ top_k ++ "}";
        try std.testing.expectError(error.ConstrainedClassificationTopKUnsupported, compile(std.testing.allocator, "{\"classifications\":[" ++ ordinary ++ "," ++ structured ++ "]}", .{}));
        try std.testing.expectError(error.ConstrainedClassificationTopKUnsupported, compile(std.testing.allocator, "{\"classifications\":[" ++ structured ++ "," ++ ordinary ++ "]}", .{}));
    }
    // Empty cross-task constraints do not enable the structured route.
    var ordinary = try compile(std.testing.allocator, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"top_k\":2}],\"classification_constraints\":[]}", .{});
    defer ordinary.deinit();
    try std.testing.expect(!usesStructuredClassification(ordinary.schema.classifications, ordinary.schema.classification_constraints.roots.len > 0));
    try std.testing.expectEqual(@as(usize, 2), ordinary.schema.classifications[0].top_k);
}

test "extraction classification top_k preserves ordinary canonical identity and legacy behavior" {
    const a = std.testing.allocator;
    var omitted = try compile(a, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"]}]}", .{});
    defer omitted.deinit();
    var explicit = try compile(a, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"top_k\":1}]}", .{});
    defer explicit.deinit();
    try std.testing.expectEqualSlices(u8, &omitted.fingerprint, &explicit.fingerprint);
    var multiple = try compile(a, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"top_k\":2}]}", .{});
    defer multiple.deinit();
    try std.testing.expect(!std.mem.eql(u8, &omitted.fingerprint, &multiple.fingerprint));
    var legacy = try compileLegacy(a, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"top_k\":2}]}", .{});
    defer legacy.deinit();
    try std.testing.expectEqual(@as(u32, 1), legacy.schema.source_version);
    try std.testing.expectEqual(@as(usize, 2), legacy.schema.classifications[0].top_k);
    // Ordinary multilabel presentation keeps its existing threshold/fallback
    // policy; only the structured route is changed by this validation.
    var multi = try compile(a, "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"multi_label\":true,\"top_k\":2}]}", .{});
    defer multi.deinit();
    try std.testing.expect(!usesStructuredClassification(multi.schema.classifications, false));
}

test "extraction classification top_k rejection releases all nested allocations" {
    const Check = struct {
        fn run(allocator: Allocator, json: []const u8) !void {
            var rejected = compile(allocator, json, .{}) catch |err| {
                if (err == error.ConstrainedClassificationTopKUnsupported) return;
                return err;
            };
            defer rejected.deinit();
            return error.TestExpectedError;
        }
    };
    for ([_][]const u8{
        "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"mode\":\"multi\",\"min_labels\":2,\"max_labels\":2,\"top_k\":1}]}",
        "{\"entities\":[\"person\"],\"entity_attributes\":{\"status\":{\"labels\":[\"active\",\"former\"]}},\"classifications\":[{\"name\":\"ordinary\",\"labels\":[\"a\",\"b\"],\"label_definitions\":{\"a\":{\"description\":\"First label\"}},\"examples\":[[\"Example\",\"a\"]],\"top_k\":2},{\"name\":\"structured\",\"labels\":[\"x\",\"y\"],\"ordered\":false}]}",
        "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"],\"top_k\":1}],\"classification_constraints\":[{\"type\":\"LabelRef\",\"task\":\"t\",\"label\":\"a\"}]}",
    }) |json| try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{json});
}

test "extraction JointIE validates typed endpoints and remains distinct" {
    const json = "{\"joint_ie\":{\"entities\":{\"person\":{},\"company\":{}},\"relations\":{\"works_for\":{\"head\":[\"person\"],\"tail\":[\"company\"],\"max_per_head\":1}},\"constraints\":[{\"type\":\"AcyclicRelation\",\"relation\":\"works_for\"},{\"type\":\"NoSelfLoops\"}]}}";
    var compiled = try compile(std.testing.allocator, json, .{});
    defer compiled.deinit();
    try std.testing.expectEqual(@as(usize, 1), compiled.schema.joint_ie.?.relations.len);
    try std.testing.expectEqual(@as(usize, 2), compiled.schema.joint_ie.?.constraints.len);
    try std.testing.expectError(error.MixedJointExtractionSchema, compile(std.testing.allocator, "{\"entities\":[\"x\"],\"joint_ie\":{\"entities\":{\"x\":{}}}}", .{}));
    try std.testing.expectError(error.UnknownExtractionReference, compile(std.testing.allocator, "{\"joint_ie\":{\"entities\":{\"x\":{}},\"relations\":{\"r\":{\"head\":[\"x\"],\"tail\":[\"missing\"]}}}}", .{}));
}

test "extraction item overrides replace entire values and depth is bounded" {
    const item = effectiveItem(.{ .schema_json = "shared", .options_json = "shared-options" }, .{ .schema_json = "{}" });
    try std.testing.expectEqualStrings("{}", item.schema_json);
    try std.testing.expectEqualStrings("shared-options", item.options_json);
    try std.testing.expectError(error.ExtractionSchemaLimitExceeded, compile(std.testing.allocator, "{\"entities\":[\"x\"]}", .{ .limits = .{ .max_json_depth = 1 } }));
    try std.testing.expectError(error.ExtractionSchemaLimitExceeded, compile(std.testing.allocator, "{\"entities\":[\"x\",\"y\"]}", .{ .limits = .{ .max_total_labels = 1 } }));
}

test "extraction fingerprints distinguish operators and prompt order" {
    const prefix = "{\"classifications\":[{\"name\":\"t\",\"labels\":[\"a\",\"b\"]}],\"classification_constraints\":[{\"type\":\"";
    const suffix = "\",\"children\":[{\"type\":\"LabelRef\",\"task\":\"t\",\"label\":\"a\"}]}]}";
    var all = try compile(std.testing.allocator, prefix ++ "And" ++ suffix, .{});
    defer all.deinit();
    var any = try compile(std.testing.allocator, prefix ++ "Or" ++ suffix, .{});
    defer any.deinit();
    try std.testing.expect(!std.mem.eql(u8, &all.fingerprint, &any.fingerprint));
    var first = try compile(std.testing.allocator, "{\"structures\":{\"s\":{\"fields\":{\"a\":\"str\",\"b\":\"str\"}}}}", .{});
    defer first.deinit();
    var second = try compile(std.testing.allocator, "{\"structures\":{\"s\":{\"fields\":{\"b\":\"str\",\"a\":\"str\"}}}}", .{});
    defer second.deinit();
    try std.testing.expect(!std.mem.eql(u8, &first.fingerprint, &second.fingerprint));
}

test "extraction compiler owns nested allocations on every failure" {
    const Check = struct {
        fn run(allocator: Allocator) !void {
            var compiled = try compile(allocator, "{\"entities\":[\"person\"],\"entity_attributes\":{\"status\":{\"labels\":[\"active\",\"former\"]}},\"classifications\":[{\"name\":\"topic\",\"labels\":[\"a\",\"b\"],\"examples\":[[\"example\",\"a\"]]}],\"classification_constraints\":[{\"type\":\"LabelRef\",\"task\":\"topic\",\"label\":\"a\"}]}", .{});
            defer compiled.deinit();
            try std.testing.expectEqual(@as(usize, 1), compiled.schema.entity_attributes.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
