// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict, owned version 2 extraction wire contract. Parsing preserves raw
//! schema presence (notably max_labels:null), and serialization is bounded
//! before allocation. Legacy extraction parsing and result shapes are separate.
const std = @import("std");
const schema = @import("../pipelines/extraction_schema.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const boundary = @import("../pipelines/gliner_boundary_decode.zig");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
const Object = std.json.ObjectMap;

pub const FailureContext = struct { input_index: ?usize = null, stage: []const u8 = "request" };
pub const Limits = struct {
    max_request_bytes: usize = 16 * 1024 * 1024,
    max_json_depth: usize = 64,
    max_inputs: usize = 128,
    max_text_bytes_per_input: usize = 1024 * 1024,
    max_total_text_bytes: usize = 16 * 1024 * 1024,
    max_total_schema_bytes: usize = 8 * 1024 * 1024,
};
pub const ParseOptions = struct {
    limits: Limits = .{},
    compiler: schema.Options = .{},
    failure: ?*FailureContext = null,
};
pub const LongDocument = struct {
    mode: enum { reject, window } = .reject,
    window_words: usize = 4096,
    overlap_words: usize = 128,
    max_windows: usize = 128,
    record_identity: @import("../pipelines/gliner_boundary_long_document.zig").RecordIdentity = .occurrence,
};
pub const Decoder = struct {
    /// Omission preserves the model's per-task default. Explicit selectors
    /// retain the native search contract, including exact-to-beam auto.
    algorithm: ?schema.constraints.Algorithm = null,
    beam_width: ?usize = null,
    max_search_nodes: usize = 200000,
    max_local_assignments: usize = 4096,
    best_effort: bool = false,
};
pub const JointOptions = struct {
    candidate_threshold: f64 = 0.05,
    entity_threshold: ?f64 = null,
    relation_role_threshold: f64 = 0.05,
    top_k_entities: usize = 32,
    top_k_roles: usize = 12,
    relation_pair_cap: usize = 128,
    max_edges_per_type: usize = 256,
    entity_weight: f64 = 1,
    relation_weight: f64 = 1,
};
pub const Options = struct {
    threshold: f32 = 0.5,
    word_splitter: processor.WordSplitter = .whitespace,
    overlap: boundary.OverlapPolicy = .flat,
    offset_unit: boundary.OffsetUnit = .utf8_bytes,
    include_confidence: bool = false,
    include_spans: bool = false,
    long_document: LongDocument = .{},
    decoder: Decoder = .{},
    joint_ie: JointOptions = .{},

    /// Beam width changes the selected witness, so an explicit unsupported
    /// width must not silently become a narrower search. Work budgets are
    /// upper bounds and may still be reduced by request-wide admission.
    pub fn validateNativeLimits(self: Options, common: pipeline.Options) !void {
        if (self.decoder.beam_width) |width| {
            if (width > common.classification_solver.beam_width or width > common.joint_solver.beam_width)
                return error.ExtractionOptionLimitExceeded;
        }
    }

    /// Splitter choice is request semantics; geometry limits and cancellation
    /// remain properties of the service-owned processor options.
    pub fn preprocessing(self: Options, common: processor.Options) processor.Options {
        var out = common;
        out.word_splitter = self.word_splitter;
        return out;
    }

    /// Resource ceilings/control/validator callbacks originate in the service.
    /// User knobs may only reduce bounded search or change decoding semantics.
    pub fn native(self: Options, common: pipeline.Options) pipeline.Options {
        var out = common;
        out.threshold = self.threshold;
        out.overlap = self.overlap;
        out.offset_unit = self.offset_unit;
        out.best_effort = self.decoder.best_effort;
        if (self.decoder.algorithm) |algorithm| {
            out.classification_solver.algorithm = algorithm;
            out.joint_solver.profile = .native;
            out.joint_solver.algorithm = switch (algorithm) {
                .exact => .exact,
                .beam => .beam,
                .auto => .auto,
            };
        }
        out.classification_solver.exact_node_budget = @min(common.classification_solver.exact_node_budget, self.decoder.max_search_nodes);
        out.classification_solver.beam_node_budget = @min(common.classification_solver.beam_node_budget, self.decoder.max_search_nodes);
        out.classification_solver.max_local_assignments = @min(common.classification_solver.max_local_assignments, self.decoder.max_local_assignments);
        out.joint_solver.exact_node_budget = @min(common.joint_solver.exact_node_budget, self.decoder.max_search_nodes);
        out.joint_solver.beam_node_budget = @min(common.joint_solver.beam_node_budget, self.decoder.max_search_nodes);
        if (self.decoder.beam_width) |width| {
            out.classification_solver.beam_width = @min(common.classification_solver.beam_width, width);
            out.joint_solver.beam_width = @min(common.joint_solver.beam_width, width);
        }
        inline for (@typeInfo(JointOptions).@"struct".fields) |field| @field(out.joint_candidates, field.name) = @field(self.joint_ie, field.name);
        return out;
    }
};
pub const Item = struct {
    id: ?[]const u8,
    text: []const u8,
    compiled: schema.CompiledSchema,
    options: Options,
};
pub const Request = struct {
    arena: std.heap.ArenaAllocator,
    model: []const u8,
    items: []Item,
    pub fn deinit(self: *Request) void {
        for (self.items) |*item| item.compiled.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn version(value: Value) !u32 {
    const object = try asObject(value);
    const raw = object.get("schema_version") orelse return 1;
    if (raw != .integer or (raw.integer != 1 and raw.integer != 2)) return error.UnsupportedExtractionSchemaVersion;
    return @intCast(raw.integer);
}

/// Bounded dispatch before a legacy generated DTO can drop unknown fields.
/// No schema compilation or model lookup occurs during the version probe.
pub fn versionJson(allocator: Allocator, bytes: []const u8, limits: Limits) !u32 {
    try scanJsonEnvelope(bytes, limits);
    var parsed = try std.json.parseFromSlice(Value, allocator, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const schema_version = try version(parsed.value);
    if (schema_version == 1) try validateLegacy(parsed.value);
    return schema_version;
}

fn rejectPresent(object: Object, names: []const []const u8) !void {
    for (names) |name| if (object.contains(name)) return error.AdvancedExtractionSchemaRequiresVersion2;
}
/// Reject only known v2 extensions. Existing v1 parsing and provider-specific
/// extensions retain their historical behavior in the legacy implementation.
pub fn validateLegacy(value: Value) !void {
    if (try version(value) != 1) return error.UnsupportedExtractionSchemaVersion;
    const object = try asObject(value);
    if (object.get("inputs")) |inputs| if (inputs == .array) {
        for (inputs.array.items) |input| if (input == .object) try rejectPresent(input.object, &.{ "schema", "options" });
    };
    if (object.get("options")) |options| if (options == .object)
        try rejectPresent(options.object, &.{ "word_splitter", "overlap", "offset_unit", "long_document", "decoder", "joint_ie" });
    const raw = object.get("schema") orelse return;
    if (raw != .object) return;
    const fields = raw.object;
    try rejectPresent(fields, &.{ "entity_definitions", "entity_attributes", "classification_constraints", "joint_ie" });
    if (fields.get("classifications")) |items| if (items == .array) {
        for (items.array.items) |item| if (item == .object)
            try rejectPresent(item.object, &.{ "label_definitions", "mode", "min_labels", "max_labels", "ordered", "threshold", "candidate_threshold", "activation", "temperature", "default", "prompt", "instruction", "examples" });
    };
    if (fields.get("relations")) |items| if (items == .array) {
        for (items.array.items) |item| if (item == .object) try rejectPresent(item.object, &.{ "description", "threshold" });
    };
    if (fields.get("structures")) |items| if (items == .object) {
        for (items.object.values()) |item| {
            if (item != .object) continue;
            try rejectPresent(item.object, &.{ "mode", "anchor", "occurrence_policy" });
            const record_fields = item.object.get("fields") orelse continue;
            if (record_fields != .object) continue;
            for (record_fields.object.values()) |field| if (field == .object)
                try rejectPresent(field.object, &.{ "dtype", "choices", "description", "threshold", "cardinality", "exclusive", "validators" });
        }
    };
}
fn asObject(value: Value) !Object {
    return if (value == .object) value.object else error.InvalidExtractionRequest;
}
fn keys(object: Object, allowed: []const []const u8) !void {
    for (object.keys()) |name| {
        var found = false;
        for (allowed) |candidate| if (std.mem.eql(u8, name, candidate)) {
            found = true;
            break;
        };
        if (!found) return error.UnknownExtractionRequestField;
    }
}
fn required(object: Object, name: []const u8) !Value {
    return object.get(name) orelse error.InvalidExtractionRequest;
}
fn string(value: Value) ![]const u8 {
    if (value != .string or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidExtractionRequest;
    return value.string;
}
fn boolValue(object: Object, name: []const u8, default: bool) !bool {
    const value = object.get(name) orelse return default;
    return if (value == .bool) value.bool else error.InvalidExtractionOptions;
}
fn number(object: Object, name: []const u8, default: f64) !f64 {
    const raw = object.get(name) orelse return default;
    const value: f64 = switch (raw) {
        .integer => @floatFromInt(raw.integer),
        .float => raw.float,
        else => return error.InvalidExtractionOptions,
    };
    if (!std.math.isFinite(value)) return error.InvalidExtractionOptions;
    return value;
}
fn probability(object: Object, name: []const u8, default: f64) !f64 {
    const value = try number(object, name, default);
    if (value < 0 or value > 1) return error.InvalidExtractionOptions;
    return value;
}
fn unsigned(object: Object, name: []const u8, default: usize, minimum: usize, maximum: usize) !usize {
    const raw = object.get(name) orelse return default;
    if (raw != .integer or raw.integer < 0) return error.InvalidExtractionOptions;
    const value = std.math.cast(usize, raw.integer) orelse return error.ExtractionOptionLimitExceeded;
    if (value < minimum or value > maximum) return error.ExtractionOptionLimitExceeded;
    return value;
}
fn enumeration(comptime T: type, object: Object, name: []const u8, default: T) !T {
    const raw = object.get(name) orelse return default;
    return std.meta.stringToEnum(T, try string(raw)) orelse error.InvalidExtractionOptions;
}
fn note(options: ParseOptions, index: ?usize, stage: []const u8) void {
    if (options.failure) |failure| failure.* = .{ .input_index = index, .stage = stage };
}

pub fn parseOptions(value: Value) !Options {
    const object = try asObject(value);
    try keys(object, &.{ "threshold", "word_splitter", "flat_ner", "overlap", "offset_unit", "include_confidence", "include_spans", "long_document", "decoder", "joint_ie", "reader", "resolver" });
    // V2 is text-native. A reader/resolver cannot be silently ignored or run
    // outside the versioned model/graph constraint contract.
    if (object.contains("reader") or object.contains("resolver")) return error.UnsupportedExtractionFeature;
    var options = Options{};
    options.threshold = @floatCast(try probability(object, "threshold", 0.5));
    options.word_splitter = try enumeration(processor.WordSplitter, object, "word_splitter", .whitespace);
    options.include_confidence = try boolValue(object, "include_confidence", false);
    options.include_spans = try boolValue(object, "include_spans", false);
    options.offset_unit = try enumeration(boundary.OffsetUnit, object, "offset_unit", .utf8_bytes);
    options.overlap = if (try boolValue(object, "flat_ner", true)) .flat else .allow;
    if (object.get("overlap")) |raw| {
        const name = try string(raw);
        const policy: boundary.OverlapPolicy = if (std.mem.eql(u8, name, "disallow")) .flat else std.meta.stringToEnum(boundary.OverlapPolicy, name) orelse return error.InvalidExtractionOptions;
        if (object.contains("flat_ner") and options.overlap != policy) return error.ConflictingExtractionOptions;
        options.overlap = policy;
    }
    if (object.get("long_document")) |raw| {
        const config = try asObject(raw);
        try keys(config, &.{ "mode", "window_words", "overlap_words", "max_windows", "record_identity" });
        options.long_document.mode = try enumeration(@FieldType(LongDocument, "mode"), config, "mode", .reject);
        options.long_document.window_words = try unsigned(config, "window_words", 4096, 1, 4096);
        options.long_document.overlap_words = try unsigned(config, "overlap_words", 128, 0, 4095);
        options.long_document.max_windows = try unsigned(config, "max_windows", 128, 1, 128);
        options.long_document.record_identity = try enumeration(@FieldType(LongDocument, "record_identity"), config, "record_identity", .occurrence);
        if (options.long_document.mode == .reject and config.count() > @intFromBool(config.contains("mode"))) return error.ConflictingExtractionOptions;
        if (options.long_document.mode == .window and options.long_document.overlap_words >= options.long_document.window_words) return error.InvalidExtractionOptions;
    }
    if (object.get("decoder")) |raw| {
        const config = try asObject(raw);
        try keys(config, &.{ "algorithm", "beam_width", "max_search_nodes", "max_local_assignments", "best_effort" });
        options.decoder.algorithm = if (config.contains("algorithm")) try enumeration(schema.constraints.Algorithm, config, "algorithm", .auto) else null;
        options.decoder.beam_width = if (config.contains("beam_width")) try unsigned(config, "beam_width", 0, 1, 1024) else null;
        options.decoder.max_search_nodes = try unsigned(config, "max_search_nodes", 200000, 0, 200000);
        options.decoder.max_local_assignments = try unsigned(config, "max_local_assignments", 4096, 1, 4096);
        options.decoder.best_effort = try boolValue(config, "best_effort", false);
    }
    if (object.get("joint_ie")) |raw| {
        const config = try asObject(raw);
        try keys(config, &.{ "candidate_threshold", "entity_threshold", "relation_role_threshold", "top_k_entities", "top_k_roles", "relation_pair_cap", "max_edges_per_type", "entity_weight", "relation_weight" });
        options.joint_ie.candidate_threshold = try probability(config, "candidate_threshold", 0.05);
        options.joint_ie.entity_threshold = if (config.contains("entity_threshold")) try probability(config, "entity_threshold", 0) else null;
        options.joint_ie.relation_role_threshold = try probability(config, "relation_role_threshold", 0.05);
        options.joint_ie.top_k_entities = try unsigned(config, "top_k_entities", 32, 1, 256);
        options.joint_ie.top_k_roles = try unsigned(config, "top_k_roles", 12, 1, 32);
        options.joint_ie.relation_pair_cap = try unsigned(config, "relation_pair_cap", 128, 1, 128);
        options.joint_ie.max_edges_per_type = try unsigned(config, "max_edges_per_type", 256, 1, 256);
        options.joint_ie.entity_weight = try number(config, "entity_weight", 1);
        options.joint_ie.relation_weight = try number(config, "relation_weight", 1);
        if (options.joint_ie.entity_weight < 0 or options.joint_ie.relation_weight < 0) return error.InvalidExtractionOptions;
    }
    return options;
}

fn textContent(allocator: Allocator, value: Value, limit: usize) ![]const u8 {
    if (value == .string) {
        const text = try string(value);
        if (text.len > limit) return error.ExtractionTextLimitExceeded;
        return allocator.dupe(u8, text);
    }
    if (value != .array or value.array.items.len == 0) return error.UnsupportedExtractionInput;
    var buffer = std.ArrayListUnmanaged(u8).empty;
    errdefer buffer.deinit(allocator);
    for (value.array.items, 0..) |part, i| {
        const object = try asObject(part);
        try keys(object, &.{ "type", "text" });
        if (!std.mem.eql(u8, try string(try required(object, "type")), "text")) return error.UnsupportedExtractionInput;
        const text = try string(try required(object, "text"));
        const fragment_bytes = std.math.add(usize, text.len, @intFromBool(i > 0)) catch return error.ExtractionTextLimitExceeded;
        const required_bytes = std.math.add(usize, buffer.items.len, fragment_bytes) catch return error.ExtractionTextLimitExceeded;
        if (required_bytes > limit) return error.ExtractionTextLimitExceeded;
        if (i > 0) try buffer.append(allocator, '\n');
        try buffer.appendSlice(allocator, text);
    }
    return buffer.toOwnedSlice(allocator);
}

/// The raw JSON parser rejects duplicate properties and excessive nesting
/// before allocating a recursive JSON tree.
pub fn scanJsonEnvelope(bytes: []const u8, limits: Limits) !void {
    if (bytes.len > limits.max_request_bytes) return error.ExtractionRequestLimitExceeded;
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |c| {
        if (quoted) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                quoted = false;
            }
        } else if (c == '"') {
            quoted = true;
        } else if (c == '{' or c == '[') {
            depth += 1;
            if (depth > limits.max_json_depth) return error.ExtractionRequestLimitExceeded;
        } else if (c == '}' or c == ']') {
            if (depth == 0) return error.InvalidExtractionRequest;
            depth -= 1;
        }
    }
    if (depth != 0 or quoted) return error.InvalidExtractionRequest;
}
pub fn parseJson(allocator: Allocator, bytes: []const u8, options: ParseOptions) !Request {
    note(options, null, "request");
    try scanJsonEnvelope(bytes, options.limits);
    var parsed = try std.json.parseFromSlice(Value, allocator, bytes, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    return parseValue(allocator, parsed.value, options);
}

pub fn parseValue(allocator: Allocator, value: Value, options: ParseOptions) !Request {
    note(options, null, "request");
    if (try version(value) != 2) return error.UnsupportedExtractionSchemaVersion;
    const object = try asObject(value);
    try keys(object, &.{ "model", "schema_version", "inputs", "schema", "options" });
    const model_name = try string(try required(object, "model"));
    if (std.mem.trim(u8, model_name, " \t\r\n").len == 0) return error.InvalidExtractionRequest;
    const inputs = try required(object, "inputs");
    if (inputs != .array or inputs.array.items.len == 0) return error.InvalidExtractionRequest;
    if (inputs.array.items.len > options.limits.max_inputs) return error.ExtractionRequestLimitExceeded;
    const shared_schema = try required(object, "schema");
    _ = try asObject(shared_schema);
    const shared_options = object.get("options") orelse Value{ .object = .empty };
    _ = try asObject(shared_options);
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const items = try owned.alloc(Item, inputs.array.items.len);
    var initialized: usize = 0;
    errdefer for (items[0..initialized]) |*item| item.compiled.deinit();
    var text_bytes: usize = 0;
    var schema_bytes: usize = 0;
    for (inputs.array.items, items, 0..) |input, *item, i| {
        note(options, i, "input");
        const fields = try asObject(input);
        try keys(fields, &.{ "id", "content", "metadata", "tokens", "schema", "options" });
        if (fields.get("metadata")) |metadata| _ = try asObject(metadata);
        if (fields.get("tokens")) |tokens| if (tokens != .array or tokens.array.items.len > 0) return error.UnsupportedExtractionInput;
        const id = if (fields.get("id")) |raw| try owned.dupe(u8, try string(raw)) else null;
        const text = try textContent(owned, try required(fields, "content"), options.limits.max_text_bytes_per_input);
        text_bytes = std.math.add(usize, text_bytes, text.len) catch return error.ExtractionTextLimitExceeded;
        if (text_bytes > options.limits.max_total_text_bytes) return error.ExtractionTextLimitExceeded;
        note(options, i, "options");
        const item_options = try parseOptions(fields.get("options") orelse shared_options);
        note(options, i, "schema");
        const raw_schema = fields.get("schema") orelse shared_schema;
        _ = try asObject(raw_schema);
        const json = try std.json.Stringify.valueAlloc(allocator, raw_schema, .{});
        defer allocator.free(json);
        schema_bytes = std.math.add(usize, schema_bytes, json.len) catch return error.ExtractionSchemaLimitExceeded;
        if (schema_bytes > options.limits.max_total_schema_bytes) return error.ExtractionSchemaLimitExceeded;
        var compiler = options.compiler;
        compiler.schema_version = 2;
        var compiled = try schema.compile(allocator, json, compiler);
        errdefer compiled.deinit();
        if (compiled.schema.joint_ie == null and (fields.get("options") orelse shared_options).object.contains("joint_ie")) return error.ConflictingExtractionOptions;
        item.* = .{ .id = id, .text = text, .compiled = compiled, .options = item_options };
        initialized += 1;
    }
    const name = try owned.dupe(u8, model_name);
    note(options, null, "request");
    return .{ .arena = arena, .model = name, .items = items };
}

pub const ErrorDetails = struct { status: u16, code: []const u8, message: []const u8 };
/// Null delegates infrastructure, model lookup, deadline and cancellation
/// errors to the existing server response machinery.
pub fn errorDetails(err: anyerror) ?ErrorDetails {
    // Keep a closed allowlist: corrupt model routing, invalid native output,
    // allocation and cancellation errors must never be relabeled client input.
    return switch (err) {
        error.LongDocumentClassificationSearchExhausted, error.LongDocumentJointSearchExhausted, error.LongDocumentRecordSearchExhausted => .{ .status = 422, .code = "EXTRACTION_SEARCH_EXHAUSTED", .message = "bounded document-level extraction search did not complete with an accepted feasible result" },
        error.LongDocumentClassificationInfeasible, error.LongDocumentJointInfeasible, error.LongDocumentRequiredRecordFieldMissing, error.AmbiguousLongDocumentRecordOccurrence => .{ .status = 422, .code = "EXTRACTION_CONSTRAINTS_INFEASIBLE", .message = "document-level extraction could not satisfy the requested record or graph constraints" },
        error.LongDocumentWindowLimitExceeded, error.LongDocumentWorkLimitExceeded, error.LongDocumentCandidateLimitExceeded, error.LongDocumentTextLimitExceeded, error.LongDocumentJointIdentityLimitExceeded => .{ .status = 413, .code = "EXTRACTION_LIMIT_EXCEEDED", .message = "document-level extraction exceeds its configured window, candidate, work, or output limit" },
        error.ClassificationSearchExhausted, error.ConstraintSearchExhausted, error.JointSearchExhausted => .{ .status = 422, .code = "EXTRACTION_SEARCH_EXHAUSTED", .message = "bounded extraction search did not complete with an accepted feasible result" },
        error.ClassificationConstraintsInfeasible, error.JointConstraintsInfeasible, error.RequiredRecordFieldMissing, error.AmbiguousRecordOccurrence => .{ .status = 422, .code = "EXTRACTION_CONSTRAINTS_INFEASIBLE", .message = "extraction could not satisfy the requested record or graph constraints" },
        error.ExtractionRequestLimitExceeded, error.ExtractionTextLimitExceeded, error.ExtractionSchemaLimitExceeded, error.ExtractionOptionLimitExceeded, error.ExtractionOutputLimitExceeded, error.ExtractionCandidateLimitExceeded, error.ExtractionRecordLimitExceeded, error.ExtractionAssignmentLimitExceeded, error.ExtractionLiteralLimitExceeded, error.ConstraintCandidateLimitExceeded, error.ConstraintLimitExceeded, error.JointCandidateLimitExceeded, error.JointScoringLimitExceeded, error.JointValidationLimitExceeded, error.BoundaryBatchLimitExceeded, error.BoundaryFragmentLimitExceeded, error.BoundaryGroupLimitExceeded, error.BoundaryQueryLimitExceeded, error.BoundarySequenceLimitExceeded, error.BoundaryTextLimitExceeded, error.RelationDedupLimitExceeded, error.RelationProposalLimitExceeded, error.EnumMatchingLimitExceeded => .{ .status = 413, .code = "EXTRACTION_LIMIT_EXCEEDED", .message = "extraction exceeds a configured text, schema, candidate, work, or output limit" },
        error.AdvancedExtractionSchemaRequiresVersion2, error.UnsupportedExtractionSchemaVersion, error.UnsupportedExtractionFeature, error.UnsupportedExtractionLongDocument, error.UnsupportedExtractionModel, error.UnsupportedExtractionBackend, error.UnsupportedGlinerBoundaryRuntime, error.UnsupportedExtractionInput, error.UnsupportedExtractionFieldType, error.UnsupportedExtractionValidator, error.UnsupportedBoundaryHypothesisTemplate, error.ConstrainedClassificationTopKUnsupported, error.UnsupportedGlinerBoundaryBackend, error.UnsupportedGlinerBoundaryTask, error.RegexValidationUnavailable => .{ .status = 400, .code = "UNSUPPORTED_EXTRACTION_FEATURE", .message = "the selected extraction version or runtime does not support a requested feature" },
        error.InvalidExtractionRequest, error.UnknownExtractionRequestField, error.InvalidExtractionOptions, error.ConflictingExtractionOptions, error.ClassificationConstraintsRequireTasks, error.ConflictingClassificationMode, error.ConflictingClassificationPrompt, error.ConflictingExtractionFieldType, error.ConflictingFieldCardinality, error.ConflictingStructureChoices, error.DuplicateClassificationTask, error.DuplicateClassificationLabel, error.DuplicateExtractionLabel, error.DuplicateRelation, error.EmptyExtractionSchema, error.EmptyStructureFields, error.EntityAttributesRequireEntities, error.EntityDefinitionsRequireEntities, error.InvalidClassificationExample, error.InvalidClassificationTopK, error.InvalidClassificationCalibration, error.InvalidClassificationCardinality, error.InvalidClassificationDefault, error.InvalidClassificationLabel, error.InvalidClassificationTask, error.InvalidExtractionBoolean, error.InvalidExtractionEnum, error.InvalidExtractionLabels, error.InvalidExtractionName, error.InvalidExtractionNumber, error.InvalidExtractionReference, error.InvalidExtractionRegex, error.InvalidExtractionSchema, error.InvalidExtractionThreshold, error.InvalidInverseRelation, error.InvalidJointCandidateLimit, error.InvalidJointConstraint, error.InvalidJointSchema, error.InvalidRelationEndpoints, error.InvalidStructureAnchor, error.InvalidSymmetricRelation, error.MissingExtractionSchemaField, error.MixedExtractionOperations, error.MixedJointExtractionSchema, error.ReservedAttributeGroup, error.ReservedExtractionMarker, error.UnknownExtractionReference, error.UnknownExtractionSchemaField, error.UnknownJointConstraintType, error.InvalidConstraint, error.InvalidConstraintCardinality, error.InvalidOrdinalConstraint, error.InvalidOrdinalTask, error.MissingClassificationDefault, error.UnknownConstraintField, error.UnknownConstraintLabel, error.UnknownConstraintTask, error.UnknownConstraintType, error.InvalidJointDecisionThreshold, error.InvalidUtf8, error.DuplicateField, error.UnexpectedToken, error.SyntaxError, error.UnexpectedEndOfInput, error.InvalidNumber, error.Overflow => .{ .status = 400, .code = "INVALID_EXTRACTION_REQUEST", .message = "extraction request, schema, references, or options are invalid" },
        error.UnsupportedExtractionRegex, error.UnsupportedExtractionRegexFlags => .{ .status = 400, .code = "UNSUPPORTED_EXTRACTION_FEATURE", .message = "the requested regex construct or flags are not supported by the bounded validator" },
        error.ExtractionRegexLimitExceeded => .{ .status = 413, .code = "EXTRACTION_LIMIT_EXCEEDED", .message = "regex validation exceeds its configured compilation or execution limit" },
        else => null,
    };
}

const BoundedWriter = struct {
    allocator: Allocator,
    limit: usize,
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    failure: ?anyerror = null,
    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *BoundedWriter = @fieldParentPtr("writer", writer);
        return self.append(data, splat) catch |err| {
            self.failure = err;
            return error.WriteFailed;
        };
    }
    fn append(self: *BoundedWriter, data: []const []const u8, splat: usize) !usize {
        var count = std.math.mul(usize, data[data.len - 1].len, splat) catch return error.ExtractionOutputLimitExceeded;
        for (data[0 .. data.len - 1]) |bytes| count = std.math.add(usize, count, bytes.len) catch return error.ExtractionOutputLimitExceeded;
        const size = std.math.add(usize, self.bytes.items.len, count) catch return error.ExtractionOutputLimitExceeded;
        if (size > self.limit) return error.ExtractionOutputLimitExceeded;
        if (size > self.bytes.capacity) {
            const grown = std.math.mul(usize, self.bytes.capacity, 2) catch self.limit;
            try self.bytes.ensureTotalCapacityPrecise(self.allocator, @min(self.limit, @max(size, @max(grown, 256))));
        }
        for (data[0 .. data.len - 1]) |bytes| self.bytes.appendSliceAssumeCapacity(bytes);
        if (data[data.len - 1].len > 0) for (0..splat) |_| self.bytes.appendSliceAssumeCapacity(data[data.len - 1]);
        return count;
    }
};

pub const ResponseWriter = struct {
    output: BoundedWriter,
    json: std.json.Stringify = undefined,
    initialized: bool = false,
    begun: bool = false,
    expected_items: usize,
    written_items: usize = 0,
    pub fn init(allocator: Allocator, max_bytes: usize, expected_items: usize) ResponseWriter {
        return .{ .output = .{ .allocator = allocator, .limit = max_bytes }, .expected_items = expected_items };
    }
    pub fn deinit(self: *ResponseWriter) void {
        self.output.bytes.deinit(self.output.allocator);
        self.* = undefined;
    }
    pub fn begin(self: *ResponseWriter, model_name: []const u8) !void {
        if (self.begun) return error.InvalidExtractionOutput;
        self.begun = true;
        self.json = .{ .writer = &self.output.writer };
        self.initialized = true;
        self.beginRaw(model_name) catch |err| return self.output.failure orelse err;
    }
    fn beginRaw(self: *ResponseWriter, model_name: []const u8) !void {
        try self.json.beginObject();
        try self.json.objectField("object");
        try self.json.write("extraction");
        try self.json.objectField("model");
        try self.json.write(model_name);
        try self.json.objectField("schema_version");
        try self.json.write(@as(u32, 2));
        try self.json.objectField("data");
        try self.json.beginArray();
    }
    pub fn append(self: *ResponseWriter, item: Item, result: pipeline.Sample) !void {
        if (!self.initialized or self.written_items >= self.expected_items) return error.InvalidExtractionOutput;
        writeSample(&self.json, item, result) catch |err| return self.output.failure orelse err;
        self.written_items += 1;
    }
    pub fn finish(self: *ResponseWriter, prompt_tokens: usize) ![]u8 {
        if (!self.initialized or self.written_items != self.expected_items) return error.InvalidExtractionOutput;
        self.finishRaw(prompt_tokens) catch |err| return self.output.failure orelse err;
        self.initialized = false;
        return self.output.bytes.toOwnedSlice(self.output.allocator);
    }
    fn finishRaw(self: *ResponseWriter, prompt_tokens: usize) !void {
        try self.json.endArray();
        try self.json.objectField("usage");
        try self.json.write(.{ .prompt_tokens = prompt_tokens, .completion_tokens = @as(usize, 0), .total_tokens = prompt_tokens });
        try self.json.endObject();
    }
};
fn score(json: *std.json.Stringify, name: []const u8, value: anytype) !void {
    if (!std.math.isFinite(value) or value < 0 or value > 1) return error.InvalidExtractionOutput;
    try json.objectField(name);
    try json.write(value);
}
fn validateSource(item: Item, source: pipeline.SourceSpan) !void {
    if (source.unit != item.options.offset_unit or source.start >= source.end or source.byte_start >= source.byte_end or source.byte_end > item.text.len) return error.InvalidExtractionOutput;
    if (item.text[source.byte_start] & 0xc0 == 0x80 or (source.byte_end < item.text.len and item.text[source.byte_end] & 0xc0 == 0x80)) return error.InvalidExtractionOutput;
    if (source.unit == .utf8_bytes and (source.start != source.byte_start or source.end != source.byte_end)) return error.InvalidExtractionOutput;
}
fn validateValue(item: Item, value: pipeline.Value, document_required: bool) !void {
    if (!std.math.isFinite(value.confidence) or value.confidence < 0 or value.confidence > 1 or !std.unicode.utf8ValidateSlice(value.text)) return error.InvalidExtractionOutput;
    if (value.source) |source| try validateSource(item, source) else if (document_required) return error.InvalidExtractionOutput;
}
fn span(json: *std.json.Stringify, item: Item, source: pipeline.SourceSpan) !void {
    try validateSource(item, source);
    try json.objectField("start");
    try json.write(source.start);
    try json.objectField("end");
    try json.write(source.end);
}
fn attributes(json: *std.json.Stringify, values: []const pipeline.Attribute) !void {
    if (values.len == 0) return;
    try json.objectField("attributes");
    try json.beginObject();
    for (values) |attribute| {
        try json.objectField(attribute.name);
        if (attribute.multi_label) try json.beginArray();
        if (!attribute.multi_label and attribute.labels.len != 1) return error.InvalidExtractionOutput;
        for (attribute.labels) |label| {
            try json.beginObject();
            try json.objectField("label");
            try json.write(label.label);
            try score(json, "confidence", label.confidence);
            try json.endObject();
        }
        if (attribute.multi_label) try json.endArray();
    }
    try json.endObject();
}
const Endpoint = struct { index: usize, label: []const u8 };
fn endpointIdentity(sample: pipeline.Sample, value: pipeline.Value, required_type: ?usize) !?Endpoint {
    const source = value.source orelse return error.InvalidExtractionOutput;
    var index: usize = 0;
    var match: ?Endpoint = null;
    var ambiguous = false;
    for (sample.entities, 0..) |group, kind| for (group.values) |candidate| {
        defer index += 1;
        if (required_type != null and kind != required_type.?) continue;
        const candidate_source = candidate.source orelse return error.InvalidExtractionOutput;
        if (source.byte_start != candidate_source.byte_start or source.byte_end != candidate_source.byte_end or !std.mem.eql(u8, value.text, candidate.text)) continue;
        if (match != null) ambiguous = true;
        match = .{ .index = index, .label = group.name };
    };
    if (required_type != null and (match == null or ambiguous)) return error.InvalidExtractionOutput;
    return if (ambiguous) null else match;
}
fn endpoint(json: *std.json.Stringify, item: Item, sample: pipeline.Sample, value: pipeline.Value, required_type: ?usize) !void {
    try validateValue(item, value, true);
    const identity = try endpointIdentity(sample, value, required_type);
    try json.beginObject();
    if (identity) |entry| {
        try json.objectField("entity_index");
        try json.write(entry.index);
        try json.objectField("label");
        try json.write(entry.label);
    }
    try json.objectField("text");
    try json.write(value.text);
    if (item.options.include_confidence) try score(json, "score", value.confidence);
    if (item.options.include_spans) try span(json, item, value.source orelse return error.InvalidExtractionOutput);
    try json.endObject();
}
fn fieldValue(json: *std.json.Stringify, item: Item, value: pipeline.Value) !void {
    try validateValue(item, value, false);
    try json.beginObject();
    try json.objectField("value");
    try json.write(value.text);
    try json.objectField("source");
    try json.write(if (value.source != null) "document" else "schema");
    if (item.options.include_confidence) try score(json, "score", value.confidence);
    if (item.options.include_spans) if (value.source) |source| try span(json, item, source);
    try json.endObject();
}
fn diagnostics(json: *std.json.Stringify, value: pipeline.Diagnostics) !void {
    if (!std.math.isFinite(value.utility)) return error.InvalidExtractionOutput;
    try json.write(.{ .status = @tagName(value.status), .utility = value.utility, .visited_nodes = value.visited_nodes, .exhausted = value.exhausted });
}
fn writeSample(json: *std.json.Stringify, item: Item, sample: pipeline.Sample) !void {
    try json.beginObject();
    if (item.id) |id| {
        try json.objectField("id");
        try json.write(id);
    }
    try json.objectField("offset_unit");
    try json.write(@tagName(item.options.offset_unit));
    if (sample.entities.len > 0) {
        try json.objectField("entities");
        try json.beginArray();
        for (sample.entities) |group| for (group.values) |value| {
            try validateValue(item, value, true);
            try json.beginObject();
            try json.objectField("label");
            try json.write(group.name);
            try json.objectField("text");
            try json.write(value.text);
            if (item.options.include_confidence) try score(json, "score", value.confidence);
            if (item.options.include_spans) try span(json, item, value.source orelse return error.InvalidExtractionOutput);
            try attributes(json, value.attributes);
            try json.endObject();
        };
        try json.endArray();
    }
    if (sample.classifications.len > 0) {
        try json.objectField("classifications");
        try json.beginArray();
        for (sample.classifications) |classification| for (classification.labels) |label| {
            try json.beginObject();
            try json.objectField("name");
            try json.write(classification.name);
            try json.objectField("label");
            try json.write(label.label);
            if (item.options.include_confidence) try score(json, "score", label.confidence);
            try json.endObject();
        };
        try json.endArray();
    }
    if (sample.relations.len > 0) {
        try json.objectField("relations");
        try json.beginArray();
        for (sample.relations) |relation| {
            try json.beginObject();
            try json.objectField("type");
            try json.write(relation.name);
            try json.objectField("source");
            try endpoint(json, item, sample, relation.head, relation.head_entity_type);
            try json.objectField("target");
            try endpoint(json, item, sample, relation.tail, relation.tail_entity_type);
            if (item.options.include_confidence) try score(json, "score", relation.confidence);
            if (relation.derived) {
                try json.objectField("derived");
                try json.write(true);
            }
            try json.endObject();
        }
        try json.endArray();
    }
    if (sample.structures.len > 0) {
        try json.objectField("structures");
        try json.beginObject();
        for (sample.structures) |structure| {
            try json.objectField(structure.name);
            try json.beginArray();
            for (structure.instances) |record| {
                try json.beginObject();
                for (record.fields) |field| {
                    if (field.dtype == .str and field.values.len > 1) return error.InvalidExtractionOutput;
                    if (field.dtype == .str and field.values.len == 0) continue;
                    try json.objectField(field.name);
                    if (field.dtype == .list) try json.beginArray();
                    for (field.values) |value| try fieldValue(json, item, value);
                    if (field.dtype == .list) try json.endArray();
                }
                try json.endObject();
            }
            try json.endArray();
        }
        try json.endObject();
        if (item.options.include_confidence or item.options.include_spans) {
            try json.objectField("structure_metadata");
            try json.beginObject();
            for (sample.structures) |structure| {
                try json.objectField(structure.name);
                try json.beginArray();
                for (structure.instances) |record| {
                    try json.beginObject();
                    if (item.options.include_confidence) if (record.confidence) |value| try score(json, "score", value);
                    if (item.options.include_spans) if (record.anchor) |anchor| {
                        try json.objectField("anchor");
                        try json.beginObject();
                        try span(json, item, anchor);
                        try json.endObject();
                    };
                    try json.endObject();
                }
                try json.endArray();
            }
            try json.endObject();
        }
    }
    if (sample.long_document) |value| {
        try json.objectField("long_document");
        try json.write(value);
    }
    if (sample.classification_solver != null or sample.joint_solver != null or sample.record_solver != null) {
        try json.objectField("solvers");
        try json.beginObject();
        if (sample.classification_solver) |value| {
            try json.objectField("classification");
            try diagnostics(json, value);
        }
        if (sample.joint_solver) |value| {
            try json.objectField("joint_ie");
            try diagnostics(json, value);
        }
        if (sample.record_solver) |value| {
            try json.objectField("records");
            try diagnostics(json, value);
        }
        try json.endObject();
    }
    try json.endObject();
}

test "extraction v2 wire versions are bounded and legacy extensions require opt in" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(@as(u32, 1), try versionJson(a, "{\"schema\":{\"entities\":[\"person\"]}}", .{}));
    try std.testing.expectEqual(@as(u32, 2), try versionJson(a, "{\"schema_version\":2}", .{}));
    try std.testing.expectError(error.UnsupportedExtractionSchemaVersion, versionJson(a, "{\"schema_version\":2.0}", .{}));
    try std.testing.expectError(error.DuplicateField, versionJson(a, "{\"schema_version\":1,\"schema_version\":2}", .{}));
    try std.testing.expectError(error.ExtractionRequestLimitExceeded, versionJson(a, "{\"nested\":[{}]}", .{ .max_json_depth = 2 }));
    try std.testing.expectError(error.ExtractionRequestLimitExceeded, versionJson(a, "{}", .{ .max_request_bytes = 1 }));
    for ([_][]const u8{
        "{\"inputs\":[{\"content\":\"x\",\"options\":{}}]}",
        "{\"options\":{\"offset_unit\":\"utf8_bytes\"}}",
        "{\"schema\":{\"classifications\":[{\"name\":\"t\",\"labels\":[\"x\"],\"ordered\":false}]}}",
        "{\"schema\":{\"structures\":{\"s\":{\"fields\":{\"a\":{\"type\":\"str\",\"exclusive\":false}}}}}}",
    }) |json| try std.testing.expectError(error.AdvancedExtractionSchemaRequiresVersion2, versionJson(a, json, .{}));
}

test "extraction v2 wire owns replacements and preserves explicit null and false" {
    const a = std.testing.allocator;
    const json =
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"options":{"threshold":0.9,"include_spans":true},"inputs":[{"id":"first","content":[{"type":"text","text":"Ada"},{"type":"text","text":"Lovelace"}]},{"id":"second","content":"hello","schema":{"classifications":[{"name":"t","labels":["a","b"],"max_labels":null,"ordered":false}]},"options":{}}]}
    ;
    const input = try a.dupe(u8, json);
    var request = try parseJson(a, input, .{});
    defer request.deinit();
    @memset(input, 'x');
    a.free(input);
    try std.testing.expectEqualStrings("m", request.model);
    try std.testing.expectEqualStrings("Ada\nLovelace", request.items[0].text);
    try std.testing.expect(request.items[0].options.include_spans);
    try std.testing.expectEqual(@as(f32, 0.9), request.items[0].options.threshold);
    try std.testing.expect(!request.items[1].options.include_spans);
    try std.testing.expectEqual(@as(f32, 0.5), request.items[1].options.threshold);
    try std.testing.expectEqual(@as(usize, 0), request.items[1].compiled.schema.entities.len);
    const classification = request.items[1].compiled.schema.classifications[0];
    try std.testing.expect(classification.structured_selection);
    try std.testing.expectEqual(@as(?usize, null), classification.task.max_labels);
    try std.testing.expect(!classification.task.ordered);
}

test "extraction v2 word splitter preserves replacement defaults and Unicode word coordinates" {
    const a = std.testing.allocator;
    var request = try parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"options":{"word_splitter":"char"},"inputs":[{"content":"東京 Ada"},{"content":"東京 Ada","options":{}},{"content":"東京 Ada","options":{"word_splitter":"whitespace"}}]}
    , .{});
    defer request.deinit();
    try std.testing.expectEqual(processor.WordSplitter.char, request.items[0].options.word_splitter);
    for (request.items[1..]) |item| try std.testing.expectEqual(processor.WordSplitter.whitespace, item.options.word_splitter);
    try std.testing.expectEqualDeep(request.items[1].options, request.items[2].options);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    for (request.items, 0..) |item, index| {
        const effective = item.options.preprocessing(.{ .max_text_words = 17, .max_sequence_tokens = 71, .control = .{ .check_fn = Cancel.check } });
        try std.testing.expectEqual(@as(usize, 17), effective.max_text_words);
        try std.testing.expectEqual(@as(usize, 71), effective.max_sequence_tokens);
        try std.testing.expectError(error.Cancelled, effective.control.?.check());
        const words = try processor.sourceWordRanges(a, item.text, .{ .word_splitter = effective.word_splitter });
        defer a.free(words);
        try std.testing.expectEqual(@as(usize, if (index == 0) 3 else 2), words.len);
        try std.testing.expectEqualStrings(if (index == 0) "東" else "東京", item.text[words[0].start..words[0].end]);
        try std.testing.expectEqualStrings("Ada", item.text[words[words.len - 1].start..words[words.len - 1].end]);
    }
}

test "extraction v2 word splitter rejects unsupported values and legacy silent fallback" {
    const a = std.testing.allocator;
    const prefix = "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"person\"]},\"inputs\":[{\"content\":\"ok\"}],\"options\":{\"word_splitter\":";
    try std.testing.expectError(error.InvalidExtractionOptions, parseJson(a, prefix ++ "\"CHAR\"}}", .{}));
    try std.testing.expectError(error.InvalidExtractionOptions, parseJson(a, prefix ++ "\"unicode\"}}", .{}));
    inline for (.{ "null", "false", "2", "[]", "{}" }) |raw|
        try std.testing.expectError(error.InvalidExtractionRequest, parseJson(a, prefix ++ raw ++ "}}", .{}));
    try std.testing.expectError(error.AdvancedExtractionSchemaRequiresVersion2, versionJson(a, "{\"options\":{\"word_splitter\":\"whitespace\"}}", .{}));
}

test "extraction v2 wire rejects atomic invalid inputs with location and limits" {
    const a = std.testing.allocator;
    var failure = FailureContext{};
    const prefix = "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\"]},\"inputs\":[{\"content\":\"ok\"},";
    try std.testing.expectError(error.EmptyExtractionSchema, parseJson(a, prefix ++ "{\"content\":\"bad\",\"schema\":{}}]}", .{ .failure = &failure }));
    try std.testing.expectEqual(@as(?usize, 1), failure.input_index);
    try std.testing.expectEqualStrings("schema", failure.stage);
    try std.testing.expectError(error.UnsupportedExtractionInput, parseJson(a, prefix ++ "{\"content\":[{\"type\":\"image_url\",\"text\":\"ignored\"}]}]}", .{ .failure = &failure }));
    try std.testing.expectEqualStrings("input", failure.stage);
    try std.testing.expectError(error.ExtractionTextLimitExceeded, parseJson(a, prefix ++ "{\"content\":\"four\"}]}", .{ .limits = .{ .max_text_bytes_per_input = 3 } }));
    try std.testing.expectError(error.ExtractionSchemaLimitExceeded, parseJson(a, prefix ++ "{\"content\":\"ok\"}]}", .{ .limits = .{ .max_total_schema_bytes = 1 } }));
    try std.testing.expectError(error.UnknownExtractionRequestField, parseJson(a, prefix ++ "{\"content\":\"ok\",\"options\":{\"ignored\":true}}]}", .{}));
    try std.testing.expectError(error.UnsupportedExtractionFeature, parseJson(a, prefix ++ "{\"content\":\"ok\",\"options\":{\"reader\":{}}}]}", .{}));
}

test "extraction v2 decoder omission preserves per-task profiles and explicit native algorithms" {
    const a = std.testing.allocator;
    var omitted = try std.json.parseFromSlice(Value, a, "{\"decoder\":{\"beam_width\":16}}", .{});
    defer omitted.deinit();
    const defaults = try parseOptions(omitted.value);
    try std.testing.expectEqual(@as(?schema.constraints.Algorithm, null), defaults.decoder.algorithm);
    const resolved = defaults.native(.{});
    try std.testing.expectEqual(.auto, resolved.classification_solver.algorithm);
    try std.testing.expectEqual(.fastino_v1, resolved.joint_solver.profile);
    try std.testing.expectEqual(.beam, resolved.joint_solver.algorithm);
    try std.testing.expectEqual(@as(usize, 16), resolved.joint_solver.beam_width);
    inline for (.{ "auto", "exact", "beam" }) |name| {
        var explicit = try std.json.parseFromSlice(Value, a, "{\"decoder\":{\"algorithm\":\"" ++ name ++ "\"}}", .{});
        defer explicit.deinit();
        const options = try parseOptions(explicit.value);
        const native = options.native(.{});
        try std.testing.expectEqual(.native, native.joint_solver.profile);
        try std.testing.expectEqualStrings(name, @tagName(native.joint_solver.algorithm));
        try std.testing.expectEqualStrings(name, @tagName(native.classification_solver.algorithm));
    }
    var explicit_null = try std.json.parseFromSlice(Value, a, "{\"decoder\":{\"algorithm\":null}}", .{});
    defer explicit_null.deinit();
    try std.testing.expectError(error.InvalidExtractionRequest, parseOptions(explicit_null.value));
}

test "extraction v2 wire exposes search ceilings and fails closed on unsupported beam width" {
    var parsed = try std.json.parseFromSlice(Value, std.testing.allocator,
        \\{"threshold":0,"flat_ner":false,"offset_unit":"utf16_codeunits","decoder":{"algorithm":"beam","beam_width":32,"max_search_nodes":0,"best_effort":true}}
    , .{});
    defer parsed.deinit();
    var options = try parseOptions(parsed.value);
    try options.validateNativeLimits(.{});
    const native = options.native(.{});
    try std.testing.expectEqual(@as(f32, 0), native.threshold);
    try std.testing.expectEqual(boundary.OverlapPolicy.allow, native.overlap);
    try std.testing.expectEqual(boundary.OffsetUnit.utf16_codeunits, native.offset_unit);
    try std.testing.expect(native.best_effort);
    try std.testing.expectEqual(@as(usize, 0), native.joint_solver.exact_node_budget);
    try std.testing.expectEqual(@as(usize, 0), native.classification_solver.beam_node_budget);
    options.decoder.beam_width = 33;
    try std.testing.expectError(error.ExtractionOptionLimitExceeded, options.validateNativeLimits(.{}));
    try std.testing.expectEqual(@as(u16, 422), errorDetails(error.ClassificationSearchExhausted).?.status);
    try std.testing.expectEqual(@as(u16, 400), errorDetails(error.MissingExtractionSchemaField).?.status);
    try std.testing.expectEqual(@as(u16, 413), errorDetails(error.ExtractionOutputLimitExceeded).?.status);
    for ([_]anyerror{ error.Cancelled, error.OutOfMemory, error.InvalidBoundaryPipelineRouting, error.InvalidExtractionOutput, error.MissingBoundaryScores, error.UnsupportedModel }) |err|
        try std.testing.expectEqual(@as(?ErrorDetails, null), errorDetails(err));
}

test "extraction v2 wire serializes attributes source-free enums and explicit offsets" {
    const a = std.testing.allocator;
    var request = try parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"options":{"include_spans":true,"offset_unit":"unicode_codepoints"},"inputs":[{"content":"😀Ada"}]}
    , .{});
    defer request.deinit();
    const source = pipeline.SourceSpan{ .start = 1, .end = 4, .unit = .unicode_codepoints, .byte_start = 4, .byte_end = 7 };
    var entity_values = [_]pipeline.Value{.{ .text = "Ada", .confidence = 0.9, .source = source, .token_span = null, .attributes = &.{ .{ .name = "status", .multi_label = false, .labels = &.{.{ .label = "active", .confidence = 0.8 }} }, .{ .name = "tags", .multi_label = true, .labels = &.{} } } }};
    const sample = pipeline.Sample{
        .entities = &.{.{ .name = "person", .dtype = .list, .values = &entity_values }},
        .classifications = &.{.{ .name = "topic", .multi_label = false, .labels = &.{.{ .label = "science", .confidence = 0.7 }} }},
        .structures = &.{.{ .name = "record", .instances = &.{.{ .anchor = source, .fields = &.{.{ .name = "status", .dtype = .str, .values = &.{.{ .text = "published", .confidence = 0.6, .source = null, .token_span = null }} }} }} }},
        .classification_solver = .{ .status = .feasible, .visited_nodes = 3, .utility = -0.2, .exhausted = true },
    };
    var writer = ResponseWriter.init(a, 4096, 1);
    defer writer.deinit();
    try writer.begin(request.model);
    try writer.append(request.items[0], sample);
    const json = try writer.finish(15);
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items[0].object;
    const entity = data.get("entities").?.array.items[0].object;
    try std.testing.expectEqual(@as(i64, 1), entity.get("start").?.integer);
    try std.testing.expect(!entity.contains("score"));
    try std.testing.expect(entity.get("attributes").?.object.get("status").?.object.contains("confidence"));
    try std.testing.expectEqual(@as(usize, 0), entity.get("attributes").?.object.get("tags").?.array.items.len);
    const field = data.get("structures").?.object.get("record").?.array.items[0].object.get("status").?.object;
    try std.testing.expectEqualStrings("schema", field.get("source").?.string);
    try std.testing.expectEqualStrings("published", field.get("value").?.string);
    try std.testing.expect(!field.contains("start") and !field.contains("end"));
    try std.testing.expect(data.get("solvers").?.object.get("classification").?.object.get("exhausted").?.bool);
    try std.testing.expectEqual(@as(i64, 15), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectError(error.InvalidExtractionOutput, writer.begin("again"));
}

test "extraction v2 wire JointIE endpoint identity retains overlapping entity types" {
    const a = std.testing.allocator;
    var request = try parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"inputs":[{"content":"Ada"}]}
    , .{});
    defer request.deinit();
    const source = pipeline.SourceSpan{ .start = 0, .end = 3, .unit = .utf8_bytes, .byte_start = 0, .byte_end = 3 };
    var values = [_]pipeline.Value{.{ .text = "Ada", .confidence = 0.9, .source = source, .token_span = null }};
    const sample = pipeline.Sample{
        .entities = &.{ .{ .name = "person", .dtype = .list, .values = &values }, .{ .name = "employee", .dtype = .list, .values = &values } },
        .relations = &.{ .{ .name = "typed", .head = values[0], .tail = values[0], .confidence = 0.8, .head_entity_type = 0, .tail_entity_type = 1 }, .{ .name = "ordinary", .head = values[0], .tail = values[0], .confidence = 0.7 } },
    };
    var writer = ResponseWriter.init(a, 4096, 1);
    defer writer.deinit();
    try writer.begin(request.model);
    try writer.append(request.items[0], sample);
    const json = try writer.finish(4);
    defer a.free(json);
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    const relations = parsed.value.object.get("data").?.array.items[0].object.get("relations").?.array.items;
    try std.testing.expectEqual(@as(i64, 0), relations[0].object.get("source").?.object.get("entity_index").?.integer);
    try std.testing.expectEqual(@as(i64, 1), relations[0].object.get("target").?.object.get("entity_index").?.integer);
    try std.testing.expect(!relations[1].object.get("source").?.object.contains("entity_index"));
    try std.testing.expectEqualStrings("Ada", relations[1].object.get("source").?.object.get("text").?.string);
    try std.testing.expect(!relations[0].object.get("source").?.object.contains("start"));
}

test "extraction v2 wire output bounds account for JSON escaping before allocation" {
    var writer = ResponseWriter.init(std.testing.allocator, 64, 0);
    defer writer.deinit();
    const control_bytes = [_]u8{1} ** 64;
    try std.testing.expectError(error.ExtractionOutputLimitExceeded, writer.begin(&control_bytes));
    try std.testing.expect(writer.output.bytes.capacity <= 64);
    try std.testing.expect(writer.output.bytes.items.len <= 64);
    var incomplete = ResponseWriter.init(std.testing.allocator, 1024, 1);
    defer incomplete.deinit();
    try incomplete.begin("m");
    try std.testing.expectError(error.InvalidExtractionOutput, incomplete.finish(0));
}

fn wireAllocationLifecycle(a: Allocator) !void {
    var request = try parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"inputs":[{"content":"Ada"},{"content":"Bob","options":{}}]}
    , .{});
    defer request.deinit();
    var writer = ResponseWriter.init(a, 4096, request.items.len);
    defer writer.deinit();
    try writer.begin(request.model);
    for (request.items) |item| try writer.append(item, .{});
    const json = try writer.finish(8);
    defer a.free(json);
}
test "extraction v2 wire allocation failures release every owned schema and response" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, wireAllocationLifecycle, .{});
}
