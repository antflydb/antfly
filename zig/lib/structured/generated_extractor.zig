// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generated entities/relations use the same immutable word-window planner
//! and source coordinates as GLiNER. The model never supplies trusted offsets
//! or confidence: alignment and decoder evidence supply them.
const std = @import("std");
const generating = @import("antfly_generating");
pub const GeneratorConfig = generating.GeneratorConfig;
const c = @import("root.zig");
const evidence = @import("root.zig").token_evidence;
const extraction_text = @import("antfly_extracting").text_processing;
const documents = extraction_text.windows;
const OffsetMap = extraction_text.OffsetMap;
const alignment = @import("source_alignment.zig");
const A = std.mem.Allocator;
const V = std.json.Value;

pub const Config = struct {
    method: enum { generated },
    generator: generating.GeneratorConfig,
    schema: V,
    entity_definitions: V = .null,
    instructions: []const u8 = "",
    output: c.Output = .auto,
    probabilities: c.Probabilities = .token_logprobs,
    batch: c.Batch = .{},
    options: Options = .{},
    pub fn validate(self: Config) !void {
        try self.generator.validate();
        try self.batch.validate();
        if (self.schema != .object or self.instructions.len > 65536) return error.InvalidGeneratedExtractorConfig;
        for (self.schema.object.keys()) |key| if (!std.mem.eql(u8, key, "entities") and !std.mem.eql(u8, key, "relations")) return error.UnsupportedGeneratedExtractionSchema;
        const entities = self.schema.object.get("entities") orelse return error.InvalidGeneratedExtractorConfig;
        if (entities != .array or entities.array.items.len == 0 or entities.array.items.len > 64) return error.InvalidGeneratedExtractorConfig;
        for (entities.array.items, 0..) |label, i| {
            if (label != .string or label.string.len == 0 or label.string.len > 256) return error.InvalidGeneratedExtractorConfig;
            for (entities.array.items[0..i]) |prior| if (std.mem.eql(u8, label.string, prior.string)) return error.InvalidGeneratedExtractorConfig;
        }
        if (self.entity_definitions != .null) {
            if (self.entity_definitions != .object) return error.InvalidGeneratedExtractorConfig;
            for (self.entity_definitions.object.keys(), self.entity_definitions.object.values()) |label, definition| {
                var found = false;
                for (entities.array.items) |entity| if (std.mem.eql(u8, label, entity.string)) {
                    found = true;
                };
                if (!found or definition != .string or definition.string.len > 16384) return error.InvalidGeneratedExtractorConfig;
            }
        }
        if (self.schema.object.get("relations")) |relations| {
            if (relations != .array or relations.array.items.len > 64) return error.InvalidGeneratedExtractorConfig;
            for (relations.array.items, 0..) |relation, i| {
                const label = try relationLabel(relation);
                for (relation.object.keys()) |key| if (!std.mem.eql(u8, key, "type") and !std.mem.eql(u8, key, "source") and !std.mem.eql(u8, key, "target") and !std.mem.eql(u8, key, "description") and !std.mem.eql(u8, key, "threshold")) return error.UnsupportedGeneratedExtractionSchema;
                if (relation.object.contains("target") and !relation.object.contains("source")) return error.InvalidGeneratedExtractorConfig;
                for ([_][]const u8{ "source", "target" }) |endpoint| if (relation.object.get(endpoint)) |value| {
                    if (value != .string) return error.InvalidGeneratedExtractorConfig;
                    var known = false;
                    for (entities.array.items) |entity| if (std.mem.eql(u8, entity.string, value.string)) {
                        known = true;
                    };
                    if (!known) return error.InvalidGeneratedExtractorConfig;
                };
                if (relation.object.get("description")) |description| if (description != .string or description.string.len > 16384) return error.InvalidGeneratedExtractorConfig;
                if (relation.object.get("threshold")) |threshold| {
                    const value: f64 = switch (threshold) {
                        .integer => @floatFromInt(threshold.integer),
                        .float => threshold.float,
                        else => return error.InvalidGeneratedExtractorConfig,
                    };
                    if (!std.math.isFinite(value) or value < 0 or value > 1 or (self.probabilities == .verdict_only and value != 0 and value != 1)) return error.InvalidGeneratedExtractorConfig;
                }
                if (label.len == 0 or label.len > 256) return error.InvalidGeneratedExtractorConfig;
                for (relations.array.items[0..i]) |previous| if (std.mem.eql(u8, label, try relationLabel(previous))) return error.InvalidGeneratedExtractorConfig;
            }
        }
        const window = self.options.long_document;
        if (window.window_words == 0 or window.window_words > 4096 or window.overlap_words >= window.window_words or window.max_windows == 0 or window.max_windows > 128) return error.InvalidGeneratedExtractorConfig;
    }
};
pub const Options = struct {
    include_confidence: bool = true,
    include_spans: bool = true,
    offset_unit: enum { utf8_bytes, unicode_codepoints } = .utf8_bytes,
    long_document: struct { mode: enum { reject, window } = .reject, window_words: usize = 1024, overlap_words: usize = 32, max_windows: usize = 128 } = .{},
};
pub fn isGenerated(a: A, raw: []const u8) !bool {
    const parsed = try std.json.parseFromSlice(V, a, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const method = parsed.value.object.get("method") orelse return false;
    return method == .string and std.mem.eql(u8, method.string, "generated");
}
pub fn parse(a: A, raw: []const u8) !std.json.Parsed(Config) {
    var parsed = try std.json.parseFromSlice(Config, a, raw, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    if (parsed.value.generator.provider != .antfly) try c.validateProviderSchema(parsed.value.generator, try c.hostedCapabilities(parsed.value.generator).select(parsed.value.output, parsed.value.probabilities), try outputSchema(parsed.arena.allocator(), parsed.value));
    return parsed;
}
fn relationLabel(value: V) ![]const u8 {
    if (value != .object) return error.InvalidGeneratedExtractorConfig;
    const label = value.object.get("type") orelse return error.InvalidGeneratedExtractorConfig;
    if (label != .string) return error.InvalidGeneratedExtractorConfig;
    return label.string;
}
fn typed(a: A, kind: []const u8) !V {
    var value = c.object();
    try c.put(a, &value, "type", .{ .string = kind });
    return value;
}
fn recordSchema(a: A, fields: V) !V {
    var record = try typed(a, "object");
    try c.put(a, &record, "properties", fields);
    var names: V = .{ .array = std.json.Array.init(a) };
    for (fields.object.keys()) |key| try names.array.append(.{ .string = key });
    try c.put(a, &record, "required", names);
    try c.put(a, &record, "additionalProperties", .{ .bool = false });
    return record;
}
fn arraySchema(a: A, entry: V, maximum: usize) !V {
    var value = try typed(a, "array");
    try c.put(a, &value, "items", entry);
    try c.put(a, &value, "maxItems", .{ .integer = @intCast(maximum) });
    return value;
}
pub fn outputSchema(a: A, config: Config) !V {
    var label = try typed(a, "string");
    try c.put(a, &label, "enum", config.schema.object.get("entities").?);
    if (config.entity_definitions != .null) try c.put(a, &label, "description", .{ .string = try std.json.Stringify.valueAlloc(a, config.entity_definitions, .{}) });
    var surface = try typed(a, "string");
    try c.put(a, &surface, "minLength", .{ .integer = 1 });
    try c.put(a, &surface, "maxLength", .{ .integer = 1024 });
    var entity = c.object();
    try c.put(a, &entity, "label", label);
    try c.put(a, &entity, "text", surface);
    try c.put(a, &entity, "present", try typed(a, "boolean"));
    var relation = c.object();
    var relation_type = try typed(a, "string");
    var labels: V = .{ .array = std.json.Array.init(a) };
    if (config.schema.object.get("relations")) |relations| for (relations.array.items) |entry| try labels.array.append(.{ .string = try relationLabel(entry) });
    if (labels.array.items.len > 0) {
        try c.put(a, &relation_type, "enum", labels);
        try c.put(a, &relation_type, "description", .{ .string = try std.json.Stringify.valueAlloc(a, config.schema.object.get("relations").?, .{}) });
    }
    try c.put(a, &relation, "type", relation_type);
    var index = try typed(a, "integer");
    try c.put(a, &index, "minimum", .{ .integer = 0 });
    try c.put(a, &index, "maximum", .{ .integer = 255 });
    try c.put(a, &relation, "source", index);
    try c.put(a, &relation, "target", index);
    try c.put(a, &relation, "present", try typed(a, "boolean"));
    var root = c.object();
    try c.put(a, &root, "entities", try arraySchema(a, try recordSchema(a, entity), 256));
    try c.put(a, &root, "relations", try arraySchema(a, try recordSchema(a, relation), if (labels.array.items.len == 0) 0 else 256));
    return recordSchema(a, root);
}

pub const Window = struct { document: usize, start: usize, end: usize };
pub const Plan = struct { windows: []Window, inputs: [][]const u8 };
pub fn plan(a: A, config: Config, texts: []const []const u8) !Plan {
    return planWithControl(a, config, texts, null);
}
pub fn planWithControl(a: A, config: Config, texts: []const []const u8, control: ?extraction_text.Checkpoint) !Plan {
    var windows: std.ArrayList(Window) = .empty;
    var inputs: std.ArrayList([]const u8) = .empty;
    const schema = try std.json.Stringify.valueAlloc(a, config.schema, .{});
    var fingerprint: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(schema, &fingerprint, .{});
    for (texts, 0..) |text, doc| {
        const policy = config.options.long_document;
        var document = try documents.plan(a, text, fingerprint, .{
            .mode = if (policy.mode == .window) .windowed else .reject,
            .max_window_body_words = policy.window_words,
            .overlap_words = policy.overlap_words,
            .max_windows = policy.max_windows,
            .control = control,
        });
        defer document.deinit();
        for (document.windows) |window| {
            try windows.append(a, .{ .document = doc, .start = window.bytes.start, .end = window.bytes.end });
            try inputs.append(a, text[window.bytes.start..window.bytes.end]);
        }
    }
    return .{ .windows = try windows.toOwnedSlice(a), .inputs = try inputs.toOwnedSlice(a) };
}

// JSON Schema integer is a mathematical type, not a dynamic Value storage tag.
// Keep the same checked conversion before acceptance and during materialization.
fn relationEndpoint(a: A, relation: V, name: []const u8) !usize {
    if (relation != .object) return error.InvalidConstrainedOutput;
    const raw = relation.object.get(name) orelse return error.InvalidConstrainedOutput;
    return @intCast(@import("antfly_jsonschema").unsignedInteger(a, raw, 255) catch |err| {
        return if (err == error.OutOfMemory) err else error.InvalidConstrainedOutput;
    });
}

/// Validate semantic indices and decoder evidence before accepting a window.
/// The common engine then repairs only this item, preserving valid siblings.
pub const Validation = struct {
    config: Config,
    pub fn validate(ptr: *anyopaque, a: A, id: usize, value: V, proof: ?c.TokenEvidence) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entities = value.object.get("entities").?.array.items;
        for (entities, 0..) |entity, i| if (entity.object.get("present").?.bool) {
            _ = try entityConfidence(a, self.config, proof, id, i, entity);
        };
        for (value.object.get("relations").?.array.items, 0..) |relation, i| if (relation.object.get("present").?.bool) {
            const source = try relationEndpoint(a, relation, "source");
            const target = try relationEndpoint(a, relation, "target");
            if (source >= entities.len or target >= entities.len) return error.InvalidConstrainedOutput;
            if (!entities[source].object.get("present").?.bool or !entities[target].object.get("present").?.bool) return error.InvalidConstrainedOutput;
            for (self.config.schema.object.get("relations").?.array.items) |definition| if (std.mem.eql(u8, try relationLabel(definition), relation.object.get("type").?.string)) {
                for ([_][]const u8{ "source", "target" }, [_]usize{ source, target }) |endpoint, entity_index| if (definition.object.get(endpoint)) |label| {
                    if (!std.mem.eql(u8, label.string, entities[entity_index].object.get("label").?.string)) return error.InvalidConstrainedOutput;
                };
            };
            _ = try relationConfidence(a, self.config, proof, id, i, relation);
        };
    }
};

/// Every source document has one typed outcome, even when its input spans
/// multiple windows. Keep execution failures with their owning document so
/// every consumer uses the same error attribution and successful siblings.
pub const Outcome = union(enum) {
    value: V,
    failure: anyerror,

    pub fn get(self: Outcome) !V {
        return switch (self) {
            .value => |value| value,
            .failure => |err| err,
        };
    }
};

pub fn materialize(a: A, config: Config, texts: []const []const u8, planned: Plan, generated: c.Result) ![]Outcome {
    return materializeWithControl(a, config, texts, planned, generated, null);
}
pub fn materializeWithControl(a: A, config: Config, texts: []const []const u8, planned: Plan, generated: c.Result, control: ?extraction_text.Checkpoint) ![]Outcome {
    const result = try a.alloc(?V, texts.len);
    const failures = try a.alloc(?anyerror, texts.len);
    const unaligned = try a.alloc(usize, texts.len);
    const entity_indices = try a.alloc(std.StringHashMapUnmanaged(usize), texts.len);
    const relation_indices = try a.alloc(std.StringHashMapUnmanaged(usize), texts.len);
    const offsets = try a.alloc(?OffsetMap, texts.len);
    for (entity_indices, relation_indices, offsets, texts) |*entities, *relations, *map, text| {
        entities.* = .empty;
        relations.* = .empty;
        map.* = if (config.options.offset_unit == .unicode_codepoints) try OffsetMap.init(a, text, 1024 * 1024) else null;
    }
    @memset(failures, null);
    @memset(unaligned, 0);
    for (result) |*output| {
        output.* = c.object();
        try c.put(a, &output.*.?, "entities", .{ .array = std.json.Array.init(a) });
        try c.put(a, &output.*.?, "relations", .{ .array = std.json.Array.init(a) });
    }
    windows_loop: for (planned.windows, generated.values, 0..) |window, value, window_index| {
        if (control) |checkpoint| try checkpoint.check();
        const document = window.document;
        const raw = value orelse {
            if (failures[document] == null) failures[document] = if (window_index < generated.failures.len)
                generated.failures[window_index] orelse error.InvalidConstrainedOutput
            else
                error.InvalidConstrainedOutput;
            continue;
        };
        const entities = raw.object.get("entities").?.array.items;
        const mentions = try a.alloc(std.ArrayList(usize), entities.len);
        for (mentions) |*indices| indices.* = .empty;
        var forms: std.ArrayList([]const u8) = .empty;
        var original_indices: std.ArrayList(usize) = .empty;
        for (entities, 0..) |entity, i| if (entity.object.get("present").?.bool) {
            try forms.append(a, entity.object.get("text").?.string);
            try original_indices.append(a, i);
        };
        const aligned = alignment.find(a, texts[document][window.start..window.end], forms.items, .{ .max_steps = 10000000, .max_mentions = 4096, .control = control }) catch |err| switch (err) {
            error.OutOfMemory, error.Cancelled, error.Canceled, error.Timeout => return err,
            else => {
                if (failures[document] == null) failures[document] = err;
                continue :windows_loop;
            },
        };
        for (entities, 0..) |entity, entity_index| {
            if (!entity.object.get("present").?.bool) continue;
            const label = entity.object.get("label").?.string;
            const confidence = try entityConfidence(a, config, generated.token_evidence[window_index], window_index, entity_index, entity);
            var found_alignment = false;
            for (aligned) |match| {
                if (original_indices.items[match.choice] != entity_index) continue;
                found_alignment = true;
                const start = window.start + match.start;
                const end = window.start + match.end;
                const output_entities = result[document].?.object.getPtr("entities").?;
                const identity = try std.fmt.allocPrint(a, "{d}:{d}:{s}", .{ start, end, label });
                const found = entity_indices[document].get(identity);
                const index_out = found orelse blk: {
                    if (output_entities.array.items.len >= 4096) {
                        failures[document] = failures[document] orelse error.GeneratedExtractionLimitExceeded;
                        continue :windows_loop;
                    }
                    var mention = c.object();
                    try c.put(a, &mention, "label", .{ .string = label });
                    try c.put(a, &mention, "text", .{ .string = texts[document][start..end] });
                    try c.put(a, &mention, "_byte_start", .{ .integer = @intCast(start) });
                    try c.put(a, &mention, "_byte_end", .{ .integer = @intCast(end) });
                    try c.put(a, &mention, "score", .{ .float = confidence });
                    try c.put(a, &mention, "confidence", .{ .float = confidence });
                    if (config.options.include_spans) {
                        try c.put(a, &mention, "start", .{ .integer = @intCast(try offset(offsets[document], start)) });
                        try c.put(a, &mention, "end", .{ .integer = @intCast(try offset(offsets[document], end)) });
                    }
                    const index_out = output_entities.array.items.len;
                    try output_entities.array.append(mention);
                    try entity_indices[document].put(a, identity, index_out);
                    break :blk index_out;
                };
                if (found != null) {
                    const mention = &output_entities.array.items[index_out];
                    const old = mention.object.get("score").?.float;
                    if (confidence > old) {
                        try c.put(a, mention, "score", .{ .float = confidence });
                        try c.put(a, mention, "confidence", .{ .float = confidence });
                    }
                }
                try mentions[entity_index].append(a, index_out);
            }
            if (!found_alignment) unaligned[document] += 1;
        }
        for (raw.object.get("relations").?.array.items, 0..) |relation, relation_index| {
            if (!relation.object.get("present").?.bool) continue;
            const source = try relationEndpoint(a, relation, "source");
            const target = try relationEndpoint(a, relation, "target");
            if (source >= mentions.len or target >= mentions.len) return error.InvalidConstrainedOutput;
            const label = relation.object.get("type").?.string;
            const confidence = try relationConfidence(a, config, generated.token_evidence[window_index], window_index, relation_index, relation);
            var threshold: f64 = 0;
            for (config.schema.object.get("relations").?.array.items) |definition| if (std.mem.eql(u8, try relationLabel(definition), label)) {
                if (definition.object.get("threshold")) |raw_threshold| threshold = if (raw_threshold == .float) raw_threshold.float else @floatFromInt(raw_threshold.integer);
            };
            if (confidence < threshold) continue;
            for (mentions[source].items) |from| for (mentions[target].items) |to| {
                const relations = result[document].?.object.getPtr("relations").?;
                const identity = try std.fmt.allocPrint(a, "{d}:{d}:{s}", .{ from, to, label });
                if (relation_indices[document].get(identity)) |existing_index| {
                    const existing = &relations.array.items[existing_index];
                    if (existing.object.get("score").?.float < confidence) try c.put(a, existing, "score", .{ .float = confidence });
                    continue;
                }
                if (relations.array.items.len >= 4096) {
                    failures[document] = failures[document] orelse error.GeneratedExtractionLimitExceeded;
                    continue :windows_loop;
                }
                var edge = c.object();
                var from_ref = c.object();
                var to_ref = c.object();
                try c.put(a, &from_ref, "entity_index", .{ .integer = @intCast(from) });
                try c.put(a, &to_ref, "entity_index", .{ .integer = @intCast(to) });
                try c.put(a, &edge, "type", .{ .string = label });
                try c.put(a, &edge, "source", from_ref);
                try c.put(a, &edge, "target", to_ref);
                try c.put(a, &edge, "score", .{ .float = confidence });
                try relation_indices[document].put(a, identity, relations.array.items.len);
                try relations.array.append(edge);
            };
        }
    }
    const outcomes = try a.alloc(Outcome, texts.len);
    for (result, 0..) |*output, i| {
        if (failures[i]) |err| {
            outcomes[i] = .{ .failure = err };
            output.* = null;
            continue;
        }
        for (output.*.?.object.getPtr("entities").?.array.items) |*entity| {
            _ = entity.object.swapRemove("_byte_start");
            _ = entity.object.swapRemove("_byte_end");
            if (!config.options.include_confidence) {
                _ = entity.object.swapRemove("score");
                _ = entity.object.swapRemove("confidence");
            }
        }
        if (!config.options.include_confidence) for (output.*.?.object.getPtr("relations").?.array.items) |*relation| {
            _ = relation.object.swapRemove("score");
        };
        try c.put(a, &output.*.?, "offset_unit", .{ .string = @tagName(config.options.offset_unit) });
        try c.put(a, &output.*.?, "unaligned_mentions", .{ .integer = @intCast(unaligned[i]) });
        try c.put(a, &output.*.?, "probability_source", .{ .string = if (config.probabilities == .verdict_only) "verdict" else "token_logprobs" });
        try c.put(a, &output.*.?, "model", .{ .string = config.generator.model });
        outcomes[i] = .{ .value = output.*.? };
    }
    return outcomes;
}
/// Encode the public all-or-error extraction response from the same typed
/// document outcomes used by embedded producers. No transport may replace a
/// failed window's cause with a generic output-validation error.
pub fn responseValue(a: A, model: []const u8, inputs: []const V, outcomes: []const Outcome, execution: c.Result) !V {
    if (inputs.len != outcomes.len) return error.InvalidGeneratedExtractorConfig;
    var data = try c.array(a, &.{});
    for (outcomes, inputs) |outcome, input| {
        var result = try outcome.get();
        if (input.object.get("id")) |id| try c.put(a, &result, "id", id);
        try data.array.append(result);
    }
    var response = c.object();
    try c.put(a, &response, "object", .{ .string = "extraction" });
    try c.put(a, &response, "schema_version", .{ .integer = 2 });
    try c.put(a, &response, "model", .{ .string = model });
    try c.put(a, &response, "data", data);
    var usage = c.object();
    try c.put(a, &usage, "input_tokens", .{ .integer = @intCast(execution.input_tokens) });
    try c.put(a, &usage, "output_tokens", .{ .integer = @intCast(execution.output_tokens) });
    try c.put(a, &response, "usage", usage);
    return response;
}

fn offset(map: ?OffsetMap, index: usize) !usize {
    return if (map) |value| (try value.convert(.{ .start = index, .end = index }, .unicode_codepoints)).start else index;
}
fn measured(a: A, proof: ?c.TokenEvidence, window: usize, path: []const []const u8, alternatives: []const V, selected: usize) !f64 {
    const p = try evidence.distribution(a, proof orelse return error.InvalidConstrainedProbabilities, window, path, alternatives);
    return p[selected];
}
fn entityConfidence(a: A, config: Config, proof: ?c.TokenEvidence, window: usize, index: usize, entity: V) !f64 {
    if (config.probabilities == .verdict_only) return 1;
    const text = try std.fmt.allocPrint(a, "{d}", .{index});
    const labels = config.schema.object.get("entities").?.array.items;
    var selected: usize = 0;
    for (labels, 0..) |label, i| if (std.mem.eql(u8, label.string, entity.object.get("label").?.string)) {
        selected = i;
    };
    return (try measured(a, proof, window, &.{ "entities", text, "label" }, labels, selected)) * (try measured(a, proof, window, &.{ "entities", text, "present" }, &.{ .{ .bool = false }, .{ .bool = true } }, 1));
}
fn relationConfidence(a: A, config: Config, proof: ?c.TokenEvidence, window: usize, index: usize, raw: V) !f64 {
    if (config.probabilities == .verdict_only) return 1;
    const text = try std.fmt.allocPrint(a, "{d}", .{index});
    const relations = config.schema.object.get("relations").?.array.items;
    const labels = try a.alloc(V, relations.len);
    var selected: usize = 0;
    for (relations, labels, 0..) |relation, *label, i| {
        label.* = .{ .string = try relationLabel(relation) };
        if (std.mem.eql(u8, label.string, raw.object.get("type").?.string)) selected = i;
    }
    return (try measured(a, proof, window, &.{ "relations", text, "type" }, labels, selected)) * (try measured(a, proof, window, &.{ "relations", text, "present" }, &.{ .{ .bool = false }, .{ .bool = true } }, 1));
}

test "asset producer runtime generated extraction aligns repeated Unicode mentions and drops hallucinations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parse(a,
        \\{"method":"generated","generator":{"provider":"antfly","model":"test"},"schema":{"entities":["person"],"relations":[]},"probabilities":"verdict_only"}
    );
    defer config.deinit();
    const value = try std.json.parseFromSliceLeaky(V, a,
        \\{"entities":[{"label":"person","text":"Éva","present":true},{"label":"person","text":"Ghost","present":true}],"relations":[]}
    , .{});
    const values = try a.alloc(?V, 2);
    @memset(values, value);
    const token_evidence = try a.alloc(?c.TokenEvidence, 2);
    @memset(token_evidence, null);
    const text = "Éva met Éva.";
    const output = try materialize(a, config.value, &.{text}, .{ .windows = try a.dupe(Window, &.{ .{ .document = 0, .start = 0, .end = text.len }, .{ .document = 0, .start = 0, .end = text.len } }), .inputs = try a.dupe([]const u8, &.{ text, text }) }, .{ .values = values, .token_evidence = token_evidence });
    const entities = (try output[0].get()).object.get("entities").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), entities.len);
    try std.testing.expectEqual(@as(i64, 0), entities[0].object.get("start").?.integer);
    try std.testing.expectEqual(@as(i64, 9), entities[1].object.get("start").?.integer);
    try std.testing.expectEqualStrings("Éva", entities[1].object.get("text").?.string);
    try std.testing.expectEqual(@as(i64, 2), (try output[0].get()).object.get("unaligned_mentions").?.integer);
}

test "generated extraction attributes typed window failures and preserves successful documents" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parse(a,
        \\{"method":"generated","generator":{"provider":"antfly","model":"fixture"},"schema":{"entities":["person"]},"probabilities":"verdict_only"}
    );
    defer config.deinit();
    const Fake = struct {
        fn invoke(_: *anyopaque, alloc: A, request: c.Invocation) !c.Reply {
            return switch (request.items[0].id) {
                0 => error.GenerationCapacityUnavailable,
                1 => error.Timeout,
                2 => error.UnsupportedConstrainedProbabilities,
                else => .{ .value = try std.json.parseFromSliceLeaky(V, alloc,
                    \\{"items":[{"item":3,"value":{"entities":[{"label":"person","text":"Alice","present":true}],"relations":[]}}]}
                , .{}) },
            };
        }
    };
    var dummy: u8 = 0;
    // Capacity and unsupported vocabulary occur at different windows of the
    // same document. First failure follows source order, not document index.
    const texts = [_][]const u8{ "Alice", "Alice", "Alice" };
    const windows = [_]Window{
        .{ .document = 0, .start = 0, .end = 5 },
        .{ .document = 0, .start = 0, .end = 5 },
        .{ .document = 1, .start = 0, .end = 5 },
        .{ .document = 2, .start = 0, .end = 5 },
    };
    const planned = Plan{ .windows = try a.dupe(Window, &windows), .inputs = try a.dupe([]const u8, &.{ "Alice", "Alice", "Alice", "Alice" }) };
    // Timeout ends a request by contract; exercise it independently below.
    const values = try std.json.parseFromSliceLeaky(V, a, "{\"entities\":[],\"relations\":[]}", .{});
    const generated = c.Result{ .values = try a.dupe(?V, &.{ null, null, null, values }), .failures = try a.dupe(?anyerror, &.{ error.GenerationCapacityUnavailable, error.Timeout, error.UnsupportedConstrainedProbabilities, null }), .token_evidence = try a.dupe(?c.TokenEvidence, &.{ null, null, null, null }) };
    const outcomes = try materialize(a, config.value, &texts, planned, generated);
    try std.testing.expectError(error.GenerationCapacityUnavailable, outcomes[0].get());
    try std.testing.expectError(error.UnsupportedConstrainedProbabilities, outcomes[1].get());
    _ = try outcomes[2].get();
    const successful_inputs = try std.json.parseFromSliceLeaky(V, a, "[{\"content\":\"Alice\",\"id\":\"stable-id\"}]", .{});
    var usage = generated;
    usage.input_tokens = 42;
    usage.output_tokens = 7;
    const response = try responseValue(a, "fixture", successful_inputs.array.items, outcomes[2..], usage);
    try std.testing.expectEqualStrings("extraction", response.object.get("object").?.string);
    try std.testing.expectEqual(@as(i64, 2), response.object.get("schema_version").?.integer);
    try std.testing.expectEqualStrings("stable-id", response.object.get("data").?.array.items[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 42), response.object.get("usage").?.object.get("input_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 7), response.object.get("usage").?.object.get("output_tokens").?.integer);
    const timed_out = try c.execute(a, .{ .ptr = &dummy, .invoke = Fake.invoke }, .grammar, try outputSchema(a, config.value), "", .{ .max_items_per_call = 1 }, planned.inputs);
    const timed_out_outcomes = try materialize(a, config.value, &texts, planned, timed_out);
    try std.testing.expectError(error.GenerationCapacityUnavailable, timed_out_outcomes[0].get());
    try std.testing.expectError(error.Timeout, timed_out_outcomes[1].get());
    try std.testing.expectError(error.Timeout, timed_out_outcomes[2].get());
}

test "generated extraction accepts mathematical integer endpoints through materialization" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parse(a,
        \\{"method":"generated","generator":{"provider":"antfly","model":"test"},"schema":{"entities":["person"],"relations":[{"type":"knows"}]},"probabilities":"verdict_only"}
    );
    defer config.deinit();
    const constrained = @import("antfly_jsonschema");
    const schema = try outputSchema(a, config.value);
    for ([_][]const u8{ "0", "0.0", "-0e10" }, [_][]const u8{ "1", "1e0", "1.00" }) |source, target| {
        const raw = try std.fmt.allocPrint(a, "{{\"entities\":[{{\"label\":\"person\",\"text\":\"Alice\",\"present\":true}},{{\"label\":\"person\",\"text\":\"Bob\",\"present\":true}}],\"relations\":[{{\"type\":\"knows\",\"source\":{s},\"target\":{s},\"present\":true}}]}}", .{ source, target });
        const value = try c.parseReply(a, raw);
        try constrained.validateJsonSchemaValue(a, schema, value);
        var validation = Validation{ .config = config.value };
        try Validation.validate(&validation, a, 0, value, null);
        const text = "Alice knows Bob";
        const result = try materialize(a, config.value, &.{text}, .{
            .windows = try a.dupe(Window, &.{.{ .document = 0, .start = 0, .end = text.len }}),
            .inputs = try a.dupe([]const u8, &.{text}),
        }, .{ .values = try a.dupe(?V, &.{value}), .token_evidence = try a.dupe(?c.TokenEvidence, &.{null}) });
        const relations = (try result[0].get()).object.get("relations").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), relations.len);
        try std.testing.expectEqual(@as(i64, 0), relations[0].object.get("source").?.object.get("entity_index").?.integer);
        try std.testing.expectEqual(@as(i64, 1), relations[0].object.get("target").?.object.get("entity_index").?.integer);
    }
    for ([_]V{ .{ .integer = -1 }, .{ .float = 0.5 }, .{ .integer = 256 }, .{ .float = std.math.inf(f64) }, .{ .number_string = "1e100000" }, .{ .string = "1" } }) |bad| {
        var relation = c.object();
        try c.put(a, &relation, "source", bad);
        try std.testing.expectError(error.InvalidConstrainedOutput, relationEndpoint(a, relation, "source"));
    }
}
