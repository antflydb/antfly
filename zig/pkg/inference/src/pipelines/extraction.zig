// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const cleanup_pipeline_mod = @import("entity_cleanup.zig");
const gliner_mod = @import("gliner.zig");
const reader_types = @import("../readers/types.zig");

const Entity = gliner_mod.Entity;
const GlinerPipeline = gliner_mod.GlinerPipeline;
const ReaderField = reader_types.Field;
const ReaderResult = reader_types.Result;
const StructuredValue = reader_types.StructuredValue;

pub const FieldType = enum {
    str,
    list,
};

pub const SchemaField = struct {
    name: []const u8,
    field_type: FieldType = .str,
    choices: []const []const u8 = &.{},

    pub fn deinit(self: *SchemaField, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.choices) |choice| allocator.free(choice);
        if (self.choices.len > 0) allocator.free(self.choices);
    }
};

pub const ExtractionSchema = struct {
    name: []const u8,
    fields: []SchemaField,

    pub fn deinit(self: *ExtractionSchema, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        for (self.fields) |*field| field.deinit(allocator);
        allocator.free(self.fields);
    }
};

pub const ExtractionConfig = struct {
    threshold: f32 = 0.3,
    flat_ner: bool = true,
    include_confidence: bool = false,
    include_spans: bool = false,
    cluster_gap: usize = 0,
    cleanup_model: ?*const cleanup_model_mod.CleanupHead = null,
};

pub const ExtractedFieldValue = struct {
    value: []const u8,
    score: ?f32 = null,
    start: ?usize = null,
    end: ?usize = null,

    pub fn deinit(self: *ExtractedFieldValue, allocator: std.mem.Allocator) void {
        allocator.free(self.value);
    }
};

pub const ExtractedField = union(enum) {
    single: ExtractedFieldValue,
    list: []ExtractedFieldValue,

    pub fn deinit(self: *ExtractedField, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .single => |*value| value.deinit(allocator),
            .list => |values| {
                for (values) |*value| value.deinit(allocator);
                allocator.free(values);
            },
        }
    }
};

pub const ExtractedFieldEntry = struct {
    name: []const u8,
    value: ExtractedField,

    pub fn deinit(self: *ExtractedFieldEntry, allocator: std.mem.Allocator) void {
        self.value.deinit(allocator);
    }
};

pub const ExtractedInstance = struct {
    fields: []ExtractedFieldEntry,

    pub fn deinit(self: *ExtractedInstance, allocator: std.mem.Allocator) void {
        for (self.fields) |*field| field.deinit(allocator);
        allocator.free(self.fields);
    }
};

pub const StructureResult = struct {
    name: []const u8,
    instances: []ExtractedInstance,

    pub fn deinit(self: *StructureResult, allocator: std.mem.Allocator) void {
        for (self.instances) |*instance| instance.deinit(allocator);
        allocator.free(self.instances);
    }
};

pub const ExtractionResult = struct {
    structures: []StructureResult,

    pub fn deinit(self: *ExtractionResult, allocator: std.mem.Allocator) void {
        for (self.structures) |*structure| structure.deinit(allocator);
        allocator.free(self.structures);
    }
};

const ResolvedSpan = struct {
    label: []const u8,
    text: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

pub fn parseSchemas(
    allocator: std.mem.Allocator,
    schema_map: *const std.json.ArrayHashMap([]const []const u8),
) ![]ExtractionSchema {
    var schemas = std.ArrayListUnmanaged(ExtractionSchema).empty;
    errdefer {
        for (schemas.items) |*schema| schema.deinit(allocator);
        schemas.deinit(allocator);
    }

    var it = schema_map.map.iterator();
    while (it.next()) |entry| {
        const struct_name = std.mem.trim(u8, entry.key_ptr.*, " \t\r\n");
        if (struct_name.len == 0) return error.EmptyStructureName;
        const field_defs = entry.value_ptr.*;
        if (field_defs.len == 0) return error.EmptyStructureFields;

        const schema_name = try allocator.dupe(u8, struct_name);
        errdefer allocator.free(schema_name);

        var fields = std.ArrayListUnmanaged(SchemaField).empty;
        errdefer {
            for (fields.items) |*field| field.deinit(allocator);
            fields.deinit(allocator);
        }

        for (field_defs) |field_def| {
            try fields.append(allocator, try parseFieldDef(allocator, field_def));
        }

        try schemas.append(allocator, .{
            .name = schema_name,
            .fields = try fields.toOwnedSlice(allocator),
        });
    }

    return try schemas.toOwnedSlice(allocator);
}

pub fn extractBatch(
    allocator: std.mem.Allocator,
    pipeline: *GlinerPipeline,
    texts: []const []const u8,
    schemas: []const ExtractionSchema,
    config: ExtractionConfig,
) ![]ExtractionResult {
    const results = try allocator.alloc(ExtractionResult, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(allocator);
        allocator.free(results);
    }

    for (texts, 0..) |text, i| {
        var structures = std.ArrayListUnmanaged(StructureResult).empty;
        errdefer {
            for (structures.items) |*structure| structure.deinit(allocator);
            structures.deinit(allocator);
        }

        for (schemas) |schema| {
            try structures.append(allocator, try extractStructure(allocator, pipeline, text, schema, config));
        }

        results[i] = .{ .structures = try structures.toOwnedSlice(allocator) };
        initialized += 1;
    }

    return results;
}

pub fn extractBatchFromReaderFields(
    allocator: std.mem.Allocator,
    batches: []const []const ReaderField,
    schemas: []const ExtractionSchema,
    config: ExtractionConfig,
) ![]ExtractionResult {
    const results = try allocator.alloc(ExtractionResult, batches.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(allocator);
        allocator.free(results);
    }

    for (batches, 0..) |fields, i| {
        var structures = std.ArrayListUnmanaged(StructureResult).empty;
        errdefer {
            for (structures.items) |*structure| structure.deinit(allocator);
            structures.deinit(allocator);
        }

        for (schemas) |schema| {
            try structures.append(allocator, try extractStructureFromReaderFields(allocator, fields, schema, config));
        }

        results[i] = .{ .structures = try structures.toOwnedSlice(allocator) };
        initialized += 1;
    }

    return results;
}

pub fn extractBatchFromReaderResults(
    allocator: std.mem.Allocator,
    batches: []const ReaderResult,
    schemas: []const ExtractionSchema,
    config: ExtractionConfig,
) ![]ExtractionResult {
    const results = try allocator.alloc(ExtractionResult, batches.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(allocator);
        allocator.free(results);
    }

    for (batches, 0..) |result, i| {
        var structures = std.ArrayListUnmanaged(StructureResult).empty;
        errdefer {
            for (structures.items) |*structure| structure.deinit(allocator);
            structures.deinit(allocator);
        }

        for (schemas) |schema| {
            if (result.structured) |*structured| {
                try structures.append(allocator, try extractStructureFromStructured(allocator, structured, schema, config));
            } else {
                try structures.append(allocator, try extractStructureFromReaderFields(allocator, result.fields, schema, config));
            }
        }

        results[i] = .{ .structures = try structures.toOwnedSlice(allocator) };
        initialized += 1;
    }

    return results;
}

fn parseFieldDef(allocator: std.mem.Allocator, def: []const u8) !SchemaField {
    const trimmed = std.mem.trim(u8, def, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyFieldDefinition;

    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(allocator);

    var iter = std.mem.splitSequence(u8, trimmed, "::");
    while (iter.next()) |part| {
        try parts.append(allocator, std.mem.trim(u8, part, " \t\r\n"));
    }

    var field = SchemaField{ .name = "" };
    var name_end = parts.items.len;
    var i = parts.items.len;
    while (i > 1) {
        i -= 1;
        const part = parts.items[i];
        if (isChoiceSpecifier(part)) {
            field.choices = try parseChoices(allocator, part);
            name_end = i;
            continue;
        }
        if (isTypeSpecifier(part)) {
            field.field_type = parseFieldType(part);
            name_end = i;
            continue;
        }
        break;
    }

    field.name = try joinFieldName(allocator, parts.items[0..name_end]);
    if (field.name.len == 0) return error.EmptyFieldName;
    return field;
}

fn extractStructure(
    allocator: std.mem.Allocator,
    pipeline: *GlinerPipeline,
    text: []const u8,
    schema: ExtractionSchema,
    config: ExtractionConfig,
) !StructureResult {
    pipeline.config.threshold = config.threshold;
    pipeline.config.flat_ner = config.flat_ner;

    const labels = try allocator.alloc([]const u8, schema.fields.len);
    defer allocator.free(labels);
    for (schema.fields, 0..) |field, i| labels[i] = field.name;

    const single_text = [_][]const u8{text};
    const entity_batches = try pipeline.recognizeBatch(&single_text, labels);
    defer {
        for (entity_batches) |entities| {
            for (entities) |entity| allocator.free(entity.text);
            allocator.free(entities);
        }
        allocator.free(entity_batches);
    }

    const raw_entities = if (entity_batches.len > 0) entity_batches[0] else &.{};
    const cleaned_entities = try applyLearnedCleanupIfPresent(allocator, config.cleanup_model, text, raw_entities);
    defer if (cleaned_entities) |entities| freeOwnedEntities(allocator, entities);

    const entities = cleaned_entities orelse raw_entities;
    if (entities.len == 0) {
        return .{
            .name = schema.name,
            .instances = try allocator.alloc(ExtractedInstance, 0),
        };
    }

    var spans = std.ArrayListUnmanaged(ResolvedSpan).empty;
    defer spans.deinit(allocator);

    for (schema.fields) |field| {
        if (field.choices.len == 0) {
            for (entities) |entity| {
                if (!std.mem.eql(u8, entity.label, field.name)) continue;
                try spans.append(allocator, .{
                    .label = field.name,
                    .text = entity.text,
                    .start = entity.start,
                    .end = entity.end,
                    .score = entity.score,
                });
            }
            continue;
        }

        var best_span: ?Entity = null;
        for (entities) |entity| {
            if (!std.mem.eql(u8, entity.label, field.name)) continue;
            if (best_span == null or entity.score > best_span.?.score) best_span = entity;
        }
        if (best_span == null) continue;

        const chosen = try resolveChoiceLabel(allocator, pipeline, best_span.?, field.choices);
        try spans.append(allocator, .{
            .label = field.name,
            .text = chosen.text,
            .start = best_span.?.start,
            .end = best_span.?.end,
            .score = chosen.score,
        });
    }

    return .{
        .name = schema.name,
        .instances = try assembleInstances(allocator, schema, spans.items, config, text.len),
    };
}

fn applyLearnedCleanupIfPresent(
    allocator: std.mem.Allocator,
    cleanup_head: ?*const cleanup_model_mod.CleanupHead,
    text: []const u8,
    entities: []const Entity,
) !?[]Entity {
    const head = cleanup_head orelse return null;

    var cleanup_entities = try allocator.alloc(cleanup_pipeline_mod.Entity, entities.len);
    defer allocator.free(cleanup_entities);
    for (entities, 0..) |entity, idx| {
        cleanup_entities[idx] = .{
            .text = entity.text,
            .label = entity.label,
            .start = entity.start,
            .end = entity.end,
            .score = entity.score,
        };
    }

    const scored = try cleanup_model_mod.scoreEntities(allocator, head, text, cleanup_entities);
    defer {
        for (scored) |*mention| mention.deinit(allocator);
        allocator.free(scored);
    }

    var cleaned = try cleanup_pipeline_mod.cleanupMentions(allocator, scored, .{
        .min_validity_score = head.min_validity_score,
        .dedup_similarity_threshold = head.dedup_similarity_threshold,
    });
    defer cleaned.deinit(allocator);

    const out = try allocator.alloc(Entity, cleaned.resolved_entities.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |entity| {
            allocator.free(entity.text);
            allocator.free(entity.label);
        }
        allocator.free(out);
    }
    for (cleaned.resolved_entities, 0..) |resolved_entity, idx| {
        out[idx] = .{
            .text = try allocator.dupe(u8, resolved_entity.text),
            .label = try allocator.dupe(u8, resolved_entity.label),
            .start = resolved_entity.start,
            .end = resolved_entity.end,
            .score = resolved_entity.detect_score * resolved_entity.validity_score,
        };
        initialized += 1;
    }
    return out;
}

fn freeOwnedEntities(allocator: std.mem.Allocator, entities: []Entity) void {
    for (entities) |entity| {
        allocator.free(entity.text);
        allocator.free(entity.label);
    }
    allocator.free(entities);
}

fn extractStructureFromReaderFields(
    allocator: std.mem.Allocator,
    fields: []const ReaderField,
    schema: ExtractionSchema,
    config: ExtractionConfig,
) !StructureResult {
    const instance = try buildInstanceFromReaderFields(allocator, schema, fields, config);
    if (instance.fields.len == 0) {
        var empty = instance;
        empty.deinit(allocator);
        return .{
            .name = schema.name,
            .instances = try allocator.alloc(ExtractedInstance, 0),
        };
    }

    const instances = try allocator.alloc(ExtractedInstance, 1);
    instances[0] = instance;
    return .{
        .name = schema.name,
        .instances = instances,
    };
}

fn extractStructureFromStructured(
    allocator: std.mem.Allocator,
    value: *const StructuredValue,
    schema: ExtractionSchema,
    config: ExtractionConfig,
) !StructureResult {
    const instance = try buildInstanceFromStructured(allocator, schema, value, config);
    if (instance.fields.len == 0) {
        var empty = instance;
        empty.deinit(allocator);
        return .{
            .name = schema.name,
            .instances = try allocator.alloc(ExtractedInstance, 0),
        };
    }

    const instances = try allocator.alloc(ExtractedInstance, 1);
    instances[0] = instance;
    return .{
        .name = schema.name,
        .instances = instances,
    };
}

fn resolveChoiceLabel(
    allocator: std.mem.Allocator,
    pipeline: *GlinerPipeline,
    span: Entity,
    choices: []const []const u8,
) !struct { text: []const u8, score: f32 } {
    for (choices) |choice| {
        if (std.ascii.eqlIgnoreCase(span.text, choice)) {
            return .{ .text = choice, .score = span.score };
        }
    }

    const single_text = [_][]const u8{span.text};
    const classifications = try pipeline.classifyBatch(&single_text, choices, .{
        .threshold = 0.0,
        .multi_label = false,
        .top_k = 1,
    });
    defer {
        for (classifications) |results| allocator.free(results);
        allocator.free(classifications);
    }

    if (classifications.len == 0 or classifications[0].len == 0) {
        return .{ .text = span.text, .score = span.score };
    }

    return .{
        .text = classifications[0][0].label,
        .score = classifications[0][0].score,
    };
}

fn assembleInstances(
    allocator: std.mem.Allocator,
    schema: ExtractionSchema,
    raw_spans: []const ResolvedSpan,
    config: ExtractionConfig,
    text_length: usize,
) ![]ExtractedInstance {
    if (raw_spans.len == 0) return try allocator.alloc(ExtractedInstance, 0);

    const spans = try allocator.dupe(ResolvedSpan, raw_spans);
    defer allocator.free(spans);

    std.mem.sort(ResolvedSpan, spans, {}, struct {
        fn lessThan(_: void, a: ResolvedSpan, b: ResolvedSpan) bool {
            return if (a.start != b.start) a.start < b.start else a.end < b.end;
        }
    }.lessThan);

    var instances = std.ArrayListUnmanaged(ExtractedInstance).empty;
    errdefer {
        for (instances.items) |*instance| instance.deinit(allocator);
        instances.deinit(allocator);
    }

    if (spans.len == 1) {
        const instance = try buildInstance(allocator, schema, spans, config);
        if (instance.fields.len > 0) try instances.append(allocator, instance) else {
            var inst = instance;
            inst.deinit(allocator);
        }
        return try instances.toOwnedSlice(allocator);
    }

    const threshold = try computeClusterThreshold(allocator, spans, config.cluster_gap, text_length);
    var cluster_start: usize = 0;
    var idx: usize = 0;
    while (idx + 1 < spans.len) : (idx += 1) {
        const gap = if (spans[idx + 1].start > spans[idx].end) spans[idx + 1].start - spans[idx].end else 0;
        if (gap > threshold) {
            const instance = try buildInstance(allocator, schema, spans[cluster_start .. idx + 1], config);
            if (instance.fields.len > 0) try instances.append(allocator, instance) else {
                var inst = instance;
                inst.deinit(allocator);
            }
            cluster_start = idx + 1;
        }
    }

    const tail = try buildInstance(allocator, schema, spans[cluster_start..], config);
    if (tail.fields.len > 0) try instances.append(allocator, tail) else {
        var t = tail;
        t.deinit(allocator);
    }

    if (instances.items.len == 0) {
        const instance = try buildInstance(allocator, schema, spans, config);
        if (instance.fields.len > 0) try instances.append(allocator, instance) else {
            var inst = instance;
            inst.deinit(allocator);
        }
    }

    return try instances.toOwnedSlice(allocator);
}

fn buildInstance(
    allocator: std.mem.Allocator,
    schema: ExtractionSchema,
    spans: []const ResolvedSpan,
    config: ExtractionConfig,
) !ExtractedInstance {
    var fields = std.ArrayListUnmanaged(ExtractedFieldEntry).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    for (schema.fields) |field| {
        switch (field.field_type) {
            .str => {
                var best_span: ?ResolvedSpan = null;
                for (spans) |span| {
                    if (!std.mem.eql(u8, span.label, field.name)) continue;
                    if (best_span == null or span.score > best_span.?.score) best_span = span;
                }
                if (best_span) |span| {
                    try fields.append(allocator, .{
                        .name = field.name,
                        .value = .{ .single = try spanToFieldValue(allocator, span, config) },
                    });
                }
            },
            .list => {
                var count: usize = 0;
                for (spans) |span| {
                    if (std.mem.eql(u8, span.label, field.name)) count += 1;
                }
                if (count == 0) continue;

                const values = try allocator.alloc(ExtractedFieldValue, count);
                var value_index: usize = 0;
                errdefer {
                    for (values[0..value_index]) |*value| value.deinit(allocator);
                    allocator.free(values);
                }

                for (spans) |span| {
                    if (!std.mem.eql(u8, span.label, field.name)) continue;
                    values[value_index] = try spanToFieldValue(allocator, span, config);
                    value_index += 1;
                }

                try fields.append(allocator, .{
                    .name = field.name,
                    .value = .{ .list = values },
                });
            },
        }
    }

    return .{ .fields = try fields.toOwnedSlice(allocator) };
}

fn buildInstanceFromReaderFields(
    allocator: std.mem.Allocator,
    schema: ExtractionSchema,
    reader_fields: []const ReaderField,
    config: ExtractionConfig,
) !ExtractedInstance {
    var fields = std.ArrayListUnmanaged(ExtractedFieldEntry).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    for (schema.fields) |field| {
        switch (field.field_type) {
            .str => {
                var best_index: ?usize = null;
                var best_quality: u8 = 0;
                for (reader_fields, 0..) |reader_field, idx| {
                    const quality = readerFieldMatchQuality(schema.name, field.name, reader_field.name);
                    if (quality == 0) continue;
                    if (best_index == null or quality > best_quality) {
                        best_index = idx;
                        best_quality = quality;
                    }
                }
                if (best_index) |idx| {
                    try fields.append(allocator, .{
                        .name = field.name,
                        .value = .{ .single = try readerFieldToValue(allocator, reader_fields[idx], config) },
                    });
                }
            },
            .list => {
                var count: usize = 0;
                for (reader_fields) |reader_field| {
                    if (readerFieldMatchQuality(schema.name, field.name, reader_field.name) > 0) count += 1;
                }
                if (count == 0) continue;

                const values = try allocator.alloc(ExtractedFieldValue, count);
                var value_index: usize = 0;
                errdefer {
                    for (values[0..value_index]) |*value| value.deinit(allocator);
                    allocator.free(values);
                }

                for (reader_fields) |reader_field| {
                    if (readerFieldMatchQuality(schema.name, field.name, reader_field.name) == 0) continue;
                    values[value_index] = try readerFieldToValue(allocator, reader_field, config);
                    value_index += 1;
                }

                try fields.append(allocator, .{
                    .name = field.name,
                    .value = .{ .list = values },
                });
            },
        }
    }

    return .{ .fields = try fields.toOwnedSlice(allocator) };
}

fn buildInstanceFromStructured(
    allocator: std.mem.Allocator,
    schema: ExtractionSchema,
    structured: *const StructuredValue,
    config: ExtractionConfig,
) !ExtractedInstance {
    var fields = std.ArrayListUnmanaged(ExtractedFieldEntry).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    for (schema.fields) |field| {
        const matched = findStructuredFieldValue(structured, schema.name, field.name) orelse continue;
        switch (field.field_type) {
            .str => {
                const value = try structuredToSingleFieldValue(allocator, matched, config) orelse continue;
                try fields.append(allocator, .{
                    .name = field.name,
                    .value = .{ .single = value },
                });
            },
            .list => {
                const values = try structuredToListFieldValues(allocator, matched, config) orelse continue;
                if (values.len == 0) {
                    allocator.free(values);
                    continue;
                }
                try fields.append(allocator, .{
                    .name = field.name,
                    .value = .{ .list = values },
                });
            },
        }
    }

    return .{ .fields = try fields.toOwnedSlice(allocator) };
}

fn spanToFieldValue(
    allocator: std.mem.Allocator,
    span: ResolvedSpan,
    config: ExtractionConfig,
) !ExtractedFieldValue {
    return .{
        .value = try allocator.dupe(u8, span.text),
        .score = if (config.include_confidence) span.score else null,
        .start = if (config.include_spans) span.start else null,
        .end = if (config.include_spans) span.end else null,
    };
}

fn readerFieldToValue(
    allocator: std.mem.Allocator,
    field: ReaderField,
    config: ExtractionConfig,
) !ExtractedFieldValue {
    _ = config;
    return .{
        .value = try allocator.dupe(u8, field.value),
        .score = null,
        .start = null,
        .end = null,
    };
}

fn structuredToSingleFieldValue(
    allocator: std.mem.Allocator,
    value: *const StructuredValue,
    config: ExtractionConfig,
) !?ExtractedFieldValue {
    return switch (value.*) {
        .null => null,
        .string => |s| ExtractedFieldValue{
            .value = try allocator.dupe(u8, s),
            .score = null,
            .start = if (config.include_spans) null else null,
            .end = if (config.include_spans) null else null,
        },
        .number => |n| ExtractedFieldValue{
            .value = try std.fmt.allocPrint(allocator, "{d}", .{n}),
            .score = null,
            .start = if (config.include_spans) null else null,
            .end = if (config.include_spans) null else null,
        },
        .boolean => |b| ExtractedFieldValue{
            .value = try allocator.dupe(u8, if (b) "true" else "false"),
            .score = null,
            .start = if (config.include_spans) null else null,
            .end = if (config.include_spans) null else null,
        },
        .array => |items| {
            for (items) |*item| {
                if (try structuredToSingleFieldValue(allocator, item, config)) |single| return single;
            }
            return null;
        },
        .object => null,
    };
}

fn structuredToListFieldValues(
    allocator: std.mem.Allocator,
    value: *const StructuredValue,
    config: ExtractionConfig,
) !?[]ExtractedFieldValue {
    switch (value.*) {
        .array => |items| {
            var out = std.ArrayListUnmanaged(ExtractedFieldValue).empty;
            errdefer {
                for (out.items) |*item| item.deinit(allocator);
                out.deinit(allocator);
            }
            for (items) |*item| {
                if (try structuredToSingleFieldValue(allocator, item, config)) |single| {
                    try out.append(allocator, single);
                }
            }
            return try out.toOwnedSlice(allocator);
        },
        else => {
            const single = try structuredToSingleFieldValue(allocator, value, config) orelse return null;
            const out = try allocator.alloc(ExtractedFieldValue, 1);
            out[0] = single;
            return out;
        },
    }
}

fn findStructuredFieldValue(
    root: *const StructuredValue,
    schema_name: []const u8,
    field_name: []const u8,
) ?*const StructuredValue {
    if (root.* != .object) return null;

    if (findObjectField(root.object, schema_name)) |schema_value| {
        if (findNamedValueRecursive(schema_value, field_name)) |value| return value;
    }
    if (findObjectField(root.object, field_name)) |value| return value;
    return findNamedValueRecursive(root, field_name);
}

fn findNamedValueRecursive(value: *const StructuredValue, name: []const u8) ?*const StructuredValue {
    switch (value.*) {
        .object => |fields| {
            if (findObjectField(fields, name)) |matched| return matched;
            for (fields) |*field| {
                if (findNamedValueRecursive(&field.value, name)) |matched| return matched;
            }
            return null;
        },
        .array => |items| {
            for (items) |*item| {
                if (findNamedValueRecursive(item, name)) |matched| return matched;
            }
            return null;
        },
        else => return null,
    }
}

fn findObjectField(fields: []const reader_types.StructuredField, name: []const u8) ?*const StructuredValue {
    for (fields) |*field| {
        if (std.ascii.eqlIgnoreCase(field.name, name)) return &field.value;
    }
    return null;
}

fn readerFieldMatchQuality(schema_name: []const u8, field_name: []const u8, candidate_name: []const u8) u8 {
    if (candidate_name.len == 0 or field_name.len == 0) return 0;
    if (std.ascii.eqlIgnoreCase(candidate_name, field_name)) return 2;

    if (lastPathSegment(candidate_name)) |last_segment| {
        if (std.ascii.eqlIgnoreCase(last_segment, field_name)) {
            const prefix_end = candidate_name.len - last_segment.len - 1;
            const prefix = candidate_name[0..prefix_end];
            if (std.ascii.eqlIgnoreCase(prefix, schema_name)) return 4;
            if (std.mem.endsWith(u8, prefix, schema_name)) return 3;
            return 1;
        }
    }

    return 0;
}

fn lastPathSegment(path: []const u8) ?[]const u8 {
    const idx = std.mem.lastIndexOfScalar(u8, path, '.') orelse return null;
    if (idx + 1 >= path.len) return null;
    return path[idx + 1 ..];
}

fn computeClusterThreshold(
    allocator: std.mem.Allocator,
    spans: []const ResolvedSpan,
    cluster_gap: usize,
    text_length: usize,
) !usize {
    if (cluster_gap > 0) return cluster_gap;
    if (spans.len <= 1) return 0;

    const gap_count = spans.len - 1;
    const gaps = try allocator.alloc(usize, gap_count);
    defer allocator.free(gaps);

    for (0..gap_count) |i| {
        gaps[i] = if (spans[i + 1].start > spans[i].end) spans[i + 1].start - spans[i].end else 0;
    }

    const median_gap = try medianInt(allocator, gaps);
    const min_gap = @min(@as(usize, 100), text_length / 10);
    return @max(median_gap * 3, min_gap);
}

fn medianInt(allocator: std.mem.Allocator, values: []usize) !usize {
    if (values.len == 0) return 0;
    const sorted = try allocator.dupe(usize, values);
    defer allocator.free(sorted);

    std.mem.sort(usize, sorted, {}, std.sort.asc(usize));

    const mid = sorted.len / 2;
    if (sorted.len % 2 == 0) {
        return sorted[mid - 1];
    }
    return sorted[mid];
}

fn isTypeSpecifier(value: []const u8) bool {
    return std.ascii.eqlIgnoreCase(value, "str") or
        std.ascii.eqlIgnoreCase(value, "string") or
        std.ascii.eqlIgnoreCase(value, "list") or
        std.ascii.eqlIgnoreCase(value, "array");
}

fn parseFieldType(value: []const u8) FieldType {
    if (std.ascii.eqlIgnoreCase(value, "list") or std.ascii.eqlIgnoreCase(value, "array")) return .list;
    return .str;
}

fn isChoiceSpecifier(value: []const u8) bool {
    return value.len >= 2 and value[0] == '[' and value[value.len - 1] == ']';
}

fn parseChoices(allocator: std.mem.Allocator, spec: []const u8) ![]const []const u8 {
    const inner = spec[1 .. spec.len - 1];
    var iter = std.mem.splitScalar(u8, inner, '|');
    var choices = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (choices.items) |choice| allocator.free(choice);
        choices.deinit(allocator);
    }

    while (iter.next()) |raw_choice| {
        const choice = std.mem.trim(u8, raw_choice, " \t\r\n");
        if (choice.len == 0) return error.EmptyChoiceValue;
        try choices.append(allocator, try allocator.dupe(u8, choice));
    }

    if (choices.items.len < 2) return error.InvalidChoiceField;
    return try choices.toOwnedSlice(allocator);
}

fn joinFieldName(allocator: std.mem.Allocator, parts: []const []const u8) ![]const u8 {
    if (parts.len == 0) return try allocator.dupe(u8, "");
    return try std.mem.join(allocator, "::", parts);
}

test "parseSchemas supports field types and choices" {
    const allocator = std.testing.allocator;

    var schema_map = std.json.ArrayHashMap([]const []const u8){};
    defer schema_map.deinit(allocator);
    try schema_map.map.put(allocator, "person", &.{
        "name::str",
        "skills::list",
        "role::[engineer|manager]::str",
    });

    const schemas = try parseSchemas(allocator, &schema_map);
    defer {
        for (schemas) |*schema| schema.deinit(allocator);
        allocator.free(schemas);
    }

    try std.testing.expectEqual(@as(usize, 1), schemas.len);
    try std.testing.expectEqualStrings("person", schemas[0].name);
    try std.testing.expectEqual(@as(usize, 3), schemas[0].fields.len);
    try std.testing.expectEqual(FieldType.str, schemas[0].fields[0].field_type);
    try std.testing.expectEqual(FieldType.list, schemas[0].fields[1].field_type);
    try std.testing.expectEqual(@as(usize, 2), schemas[0].fields[2].choices.len);
    try std.testing.expectEqualStrings("engineer", schemas[0].fields[2].choices[0]);
    try std.testing.expectEqualStrings("manager", schemas[0].fields[2].choices[1]);
}

test "parseSchemas preserves names containing double colons" {
    const allocator = std.testing.allocator;

    var schema_map = std.json.ArrayHashMap([]const []const u8){};
    defer schema_map.deinit(allocator);
    try schema_map.map.put(allocator, "record", &.{"person::name::str"});

    const schemas = try parseSchemas(allocator, &schema_map);
    defer {
        for (schemas) |*schema| schema.deinit(allocator);
        allocator.free(schemas);
    }

    try std.testing.expectEqual(@as(usize, 1), schemas.len);
    try std.testing.expectEqual(@as(usize, 1), schemas[0].fields.len);
    try std.testing.expectEqualStrings("person::name", schemas[0].fields[0].name);
}

test "assembleInstances clusters spans into structured instances" {
    const allocator = std.testing.allocator;

    var schema_fields = [_]SchemaField{
        .{ .name = "name", .field_type = .str },
        .{ .name = "skills", .field_type = .list },
    };
    const schema = ExtractionSchema{
        .name = "person",
        .fields = schema_fields[0..],
    };
    const spans = [_]ResolvedSpan{
        .{ .label = "name", .text = "Alice", .start = 0, .end = 5, .score = 0.9 },
        .{ .label = "skills", .text = "Go", .start = 12, .end = 14, .score = 0.8 },
        .{ .label = "name", .text = "Bob", .start = 180, .end = 183, .score = 0.95 },
    };

    const instances = try assembleInstances(allocator, schema, &spans, .{}, 240);
    defer {
        for (instances) |*instance| instance.deinit(allocator);
        allocator.free(instances);
    }

    try std.testing.expectEqual(@as(usize, 2), instances.len);
    try std.testing.expectEqual(@as(usize, 2), instances[0].fields.len);
    try std.testing.expectEqual(@as(usize, 1), instances[1].fields.len);
    try std.testing.expectEqualStrings("Alice", instances[0].fields[0].value.single.value);
    try std.testing.expectEqualStrings("Bob", instances[1].fields[0].value.single.value);
}

test "extractBatchFromReaderFields maps structured reader fields into schema instances" {
    const allocator = std.testing.allocator;

    var schema_map = std.json.ArrayHashMap([]const []const u8){};
    defer schema_map.deinit(allocator);
    try schema_map.map.put(allocator, "person", &.{
        "name::str",
        "skills::list",
    });

    const schemas = try parseSchemas(allocator, &schema_map);
    defer {
        for (schemas) |*schema| schema.deinit(allocator);
        allocator.free(schemas);
    }

    const reader_fields = [_]ReaderField{
        .{ .name = "person.name", .value = "Alice" },
        .{ .name = "person.skills", .value = "Go" },
        .{ .name = "person.skills", .value = "Zig" },
        .{ .name = "company.name", .value = "Example Corp" },
    };
    const batches = [_][]const ReaderField{reader_fields[0..]};

    const results = try extractBatchFromReaderFields(allocator, &batches, schemas, .{});
    defer {
        for (results) |*result| result.deinit(allocator);
        allocator.free(results);
    }

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 1), results[0].structures.len);
    try std.testing.expectEqual(@as(usize, 1), results[0].structures[0].instances.len);
    try std.testing.expectEqual(@as(usize, 2), results[0].structures[0].instances[0].fields.len);
    try std.testing.expectEqualStrings("Alice", results[0].structures[0].instances[0].fields[0].value.single.value);
    try std.testing.expectEqual(@as(usize, 2), results[0].structures[0].instances[0].fields[1].value.list.len);
    try std.testing.expectEqualStrings("Go", results[0].structures[0].instances[0].fields[1].value.list[0].value);
    try std.testing.expectEqualStrings("Zig", results[0].structures[0].instances[0].fields[1].value.list[1].value);
}

test "readerFieldMatchQuality prefers schema-qualified paths" {
    try std.testing.expectEqual(@as(u8, 4), readerFieldMatchQuality("person", "name", "person.name"));
    try std.testing.expectEqual(@as(u8, 1), readerFieldMatchQuality("person", "name", "company.name"));
    try std.testing.expectEqual(@as(u8, 2), readerFieldMatchQuality("person", "name", "name"));
    try std.testing.expectEqual(@as(u8, 0), readerFieldMatchQuality("person", "name", "person.age"));
}

test "extractBatchFromReaderResults prefers structured payloads" {
    const allocator = std.testing.allocator;

    var schema_map = std.json.ArrayHashMap([]const []const u8){};
    defer schema_map.deinit(allocator);
    try schema_map.map.put(allocator, "person", &.{
        "name::str",
        "skills::list",
    });

    const schemas = try parseSchemas(allocator, &schema_map);
    defer {
        for (schemas) |*schema| schema.deinit(allocator);
        allocator.free(schemas);
    }

    const structured = StructuredValue{
        .object = try allocator.dupe(reader_types.StructuredField, &.{
            .{
                .name = try allocator.dupe(u8, "person"),
                .value = .{ .object = try allocator.dupe(reader_types.StructuredField, &.{
                    .{ .name = try allocator.dupe(u8, "name"), .value = .{ .string = try allocator.dupe(u8, "Alice") } },
                    .{ .name = try allocator.dupe(u8, "skills"), .value = .{ .array = try allocator.dupe(StructuredValue, &.{
                        .{ .string = try allocator.dupe(u8, "Go") },
                        .{ .string = try allocator.dupe(u8, "Zig") },
                    }) } },
                }) },
            },
        }),
    };

    var result = ReaderResult{
        .text = try allocator.dupe(u8, "Alice"),
        .fields = try allocator.dupe(ReaderField, &.{
            .{ .name = try allocator.dupe(u8, "name"), .value = try allocator.dupe(u8, "Fallback") },
        }),
        .structured = structured,
        .allocator = allocator,
    };
    defer result.deinit();

    const outputs = try extractBatchFromReaderResults(allocator, &.{result}, schemas, .{});
    defer {
        for (outputs) |*output| output.deinit(allocator);
        allocator.free(outputs);
    }

    try std.testing.expectEqualStrings("Alice", outputs[0].structures[0].instances[0].fields[0].value.single.value);
    try std.testing.expectEqual(@as(usize, 2), outputs[0].structures[0].instances[0].fields[1].value.list.len);
}
