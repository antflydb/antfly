// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict schema-aware GLiNER2.5 training annotations and packed targets.
//! This canonical layer accepts explicit offsets/choice identities. Importers
//! must resolve string-only occurrences before entry; no target is snapped,
//! silently dropped, inferred from an evaluation answer, or truncated.
const std = @import("std");
const Allocator = std.mem.Allocator;
const schema_mod = @import("../pipelines/extraction_schema.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const offsets_mod = @import("../pipelines/gliner_boundary_decode.zig");
const unicode = @import("../pipelines/gliner_boundary_unicode.zig");
const constraints = @import("../pipelines/extraction_constraints.zig");
const joint = @import("../pipelines/extraction_joint_ie.zig");
const loss = @import("gliner_boundary_losses.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Span = loss.Span;
pub const Source = struct { start: usize, end: usize, unit: offsets_mod.OffsetUnit = .utf8_bytes };
pub const Attribute = struct { group: usize, labels: []const usize };
pub const Entity = struct { entity_type: usize, source: Source, attributes: []const Attribute = &.{} };
pub const Classification = struct { task: usize, labels: []const usize };
/// One distinct value can have several explicit alternative occurrences.
/// A choice points to the declared enum and its synthetic scoring position.
pub const Value = union(enum) { document: []const Source, choice: usize };
pub const Field = struct { field: usize, values: []const Value };
pub const Record = struct { structure: usize, id: []const u8, fields: []const Field };
pub const Endpoint = union(enum) { document: Source, entity: usize };
pub const Relation = struct { relation_type: usize, head: Endpoint, tail: Endpoint };
pub const Annotations = struct {
    schema_fingerprint: [32]u8,
    /// Entity, attribute, record, and relation annotations are complete for
    /// the declared schema. Absent extractive labels mean negative targets.
    entities: []const Entity = &.{},
    records: []const Record = &.{},
    relations: []const Relation = &.{},
    /// Omitted classification tasks are unsupervised. A supplied empty set is
    /// an explicit negative target and must satisfy that task's cardinality.
    classifications: []const Classification = &.{},
};
pub const Failure = struct { sample: ?usize = null, annotation: ?usize = null, stage: []const u8 = "preflight" };
pub const Options = struct {
    max_batch: usize = 128,
    max_text_bytes: usize = 1024 * 1024,
    max_total_text_bytes: usize = 16 * 1024 * 1024,
    max_words: usize = 16384,
    max_queries: usize = 1024,
    max_classification_labels: usize = 4096,
    max_annotations: usize = 65536,
    max_gold_per_query: usize = 256,
    /// Null pads only to the observed maximum, with a minimum of one. An
    /// explicit capacity preserves fixed-shape training without truncation.
    gold_capacity: ?usize = null,
    max_padded_elements: usize = 16 * 1024 * 1024,
    max_record_id_bytes: usize = 256,
    max_work: usize = 20000000,
    regex_context: ?*anyopaque = null,
    validate_value_fn: ?*const fn (?*anyopaque, schema_mod.RegexValidator, []const u8) anyerror!bool = null,
    joint_limits: joint.Options = .{},
    control: ?Control = null,
    failure: ?*Failure = null,
};
pub const RecordValue = struct { alternatives: []const Span, choice: ?usize = null };
pub const RecordField = struct { field: usize, query: usize, values: []const RecordValue };
pub const RecordTarget = struct {
    structure: usize,
    group: usize,
    id: []const u8,
    anchor_query: ?usize,
    fields: []const RecordField,
};
/// Pinned processing.records._default_cardinality: explicit cardinality wins;
/// otherwise anchors are required scalars, strings optional scalars and lists
/// zero-or-more. Alternative source occurrences remain one distinct value.
pub fn fieldCardinality(definition: schema_mod.Field, is_anchor: bool) schema_mod.Cardinality {
    return definition.cardinality orelse if (is_anchor) .required_one else if (definition.dtype == .str) .optional_one else .zero_or_more;
}
pub const RelationTarget = struct {
    relation_type: usize,
    head_query: usize,
    tail_query: usize,
    head: Span,
    tail: Span,
    head_entity: ?usize,
    tail_entity: ?usize,
};
pub const Sample = struct {
    schema_fingerprint: [32]u8,
    text_fingerprint: [32]u8,
    word_count: usize,
    /// Gold pairs remain grouped by exact routed query in stable input order.
    mentions: []const []const Span,
    entity_spans: []const Span,
    records: []const RecordTarget,
    relations: []const RelationTarget,
};
pub const Targets = struct {
    arena: std.heap.ArenaAllocator,
    samples: []const Sample,
    word_width: usize,
    query_width: usize,
    classification_width: usize,
    gold_capacity: usize,
    /// [B,Q,G] in the same coordinates used by prepared text states; synthetic
    /// enum-prefix positions are included, document offsets remain separate.
    mention_pairs: []const Span,
    mention_mask: []const bool,
    query_mask: []const bool,
    classification_targets: []const f32,
    classification_mask: []const bool,
    work: usize,
    pub fn deinit(self: *Targets) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn gold(self: *const Targets) loss.Gold {
        return .{ .batch = self.samples.len, .queries = self.query_width, .capacity = self.gold_capacity, .spans = self.mention_pairs, .valid = self.mention_mask };
    }
};
const DocumentWord = struct { start: usize, end: usize, word: usize };
const Compiler = struct {
    allocator: Allocator,
    scratch: Allocator,
    options: Options,
    work: usize = 0,
    annotations: usize = 0,
    fn tick(self: *Compiler) !void {
        if (self.work >= self.options.max_work) return error.BoundaryTrainingLimitExceeded;
        self.work += 1;
        if (self.work % 128 == 1) try self.check();
    }
    fn check(self: *const Compiler) !void {
        if (self.options.control) |control| try control.check();
    }
    fn charge(self: *Compiler, n: usize) !void {
        if (n > self.options.max_annotations -| self.annotations) return error.BoundaryTrainingLimitExceeded;
        self.annotations += n;
        try self.tick();
    }
    fn progress(self: *Compiler, sample: usize, annotation: ?usize, stage: []const u8) !void {
        try self.check();
        if (self.options.failure) |failure| failure.* = .{ .sample = sample, .annotation = annotation, .stage = stage };
    }
    fn route(self: *Compiler, sample: processor.Sample, kind: processor.QueryKind, schema_index: usize, label_index: ?usize) !usize {
        var found: ?usize = null;
        for (sample.queries, 0..) |query, index| {
            try self.tick();
            if (query.kind == kind and query.schema_index == schema_index and (label_index == null or query.label_index == label_index.?)) {
                if (found != null or query.group_index >= sample.groups.len) return error.InvalidBoundaryTrainingRouting;
                found = index;
            }
        }
        return found orelse error.InvalidBoundaryTrainingRouting;
    }
    fn validateRoutes(self: *Compiler, sample: processor.Sample, schema: schema_mod.Schema) !void {
        var count: usize = 0;
        const entity_count = if (schema.joint_ie) |j| j.entities.len else schema.entities.len;
        const relation_count = if (schema.joint_ie) |j| j.relations.len else schema.relations.len;
        for (0..entity_count) |e| {
            _ = try self.route(sample, .entity, e, null);
            count += 1;
        }
        for (schema.entity_attributes, 0..) |group, g| for (group.labels, 0..) |_, label| {
            _ = try self.route(sample, .attribute, g, label);
            count += 1;
        };
        for (schema.structures, 0..) |structure, s| for (structure.fields, 0..) |_, f| {
            _ = try self.route(sample, .field, s, f);
            count += 1;
        };
        for (0..relation_count) |r| {
            _ = try self.route(sample, .relation_head, r, null);
            _ = try self.route(sample, .relation_tail, r, null);
            count += 2;
        }
        if (count != sample.queries.len or (schema.joint_ie != null) != sample.is_joint_ie) return error.InvalidBoundaryTrainingRouting;
        var label_count: usize = 0;
        for (schema.classifications, 0..) |task, t| for (task.task.labels, 0..) |_, label| {
            _ = try self.classRoute(sample, t, label);
            label_count += 1;
        };
        if (label_count != sample.classification_labels.len) return error.InvalidBoundaryTrainingRouting;
    }
    fn classRoute(self: *Compiler, sample: processor.Sample, task: usize, label: usize) !usize {
        var found: ?usize = null;
        for (sample.classification_labels, 0..) |query, index| {
            try self.tick();
            if (query.schema_index == task and query.label_index == label) {
                if (found != null or query.group_index >= sample.groups.len) return error.InvalidBoundaryTrainingRouting;
                found = index;
            }
        }
        return found orelse error.InvalidBoundaryTrainingRouting;
    }
    fn documentWords(self: *Compiler, sample: processor.Sample) ![]DocumentWord {
        if (sample.prefix_word_count > sample.words.len or sample.body_word_count != sample.words.len - sample.prefix_word_count)
            return error.InvalidBoundaryTrainingRouting;
        var words = std.ArrayListUnmanaged(DocumentWord).empty;
        errdefer words.deinit(self.scratch);
        for (sample.words, 0..) |word, index| {
            try self.tick();
            if (word.source) |source| {
                if (index < sample.prefix_word_count or source.start >= source.end or source.end > sample.original_text.len or
                    (words.items.len > 0 and source.start < words.items[words.items.len - 1].end)) return error.InvalidBoundaryTrainingRouting;
                try words.append(self.scratch, .{ .start = source.start, .end = source.end, .word = index });
            } else if (index >= sample.prefix_word_count and index + 1 != sample.words.len) return error.InvalidBoundaryTrainingRouting;
        }
        return words.toOwnedSlice(self.scratch);
    }
    fn mapSource(self: *Compiler, source: Source, mapping: offsets_mod.OffsetMap, words: []const DocumentWord) !Span {
        try self.tick();
        const bytes = try mapping.toBytes(.{ .start = source.start, .end = source.end }, source.unit);
        if (bytes.start >= bytes.end) return error.InvalidBoundaryTrainingTargets;
        var lo: usize = 0;
        var hi = words.len;
        while (lo < hi) {
            try self.tick();
            const mid = lo + (hi - lo) / 2;
            if (words[mid].start < bytes.start) lo = mid + 1 else hi = mid;
        }
        if (lo >= words.len or words[lo].start != bytes.start) return error.UnalignedBoundaryTrainingTarget;
        const start = words[lo].word;
        hi = words.len;
        while (lo < hi) {
            try self.tick();
            const mid = lo + (hi - lo) / 2;
            if (words[mid].end < bytes.end) lo = mid + 1 else hi = mid;
        }
        if (lo >= words.len or words[lo].end != bytes.end) return error.UnalignedBoundaryTrainingTarget;
        return .{ .start = @intCast(start), .end = @intCast(words[lo].word + 1) };
    }
    fn addMention(self: *Compiler, mentions: []std.ArrayListUnmanaged(Span), query: usize, span: Span) !void {
        if (query >= mentions.len) return error.InvalidBoundaryTrainingRouting;
        for (mentions[query].items) |existing| {
            try self.tick();
            if (sameSpan(existing, span)) return;
        }
        if (mentions[query].items.len >= self.options.max_gold_per_query) return error.BoundaryTrainingTargetCapacityExceeded;
        try mentions[query].append(self.allocator, span);
    }
    fn validators(self: *Compiler, list: []const schema_mod.RegexValidator, surface_: []const u8) !void {
        if (list.len == 0) return;
        const callback = self.options.validate_value_fn orelse return error.MissingExtractionValidator;
        for (list) |validator| {
            try self.tick();
            if (!try callback(self.options.regex_context, validator, surface_)) return error.InvalidBoundaryTrainingTargets;
        }
    }
    fn surface(_: *Compiler, sample: processor.Sample, span: Span) ![]const u8 {
        if (span.start < 0 or span.end <= span.start or span.end > sample.words.len) return error.InvalidBoundaryTrainingTargets;
        const start = sample.words[@intCast(span.start)].source orelse return error.InvalidBoundaryTrainingTargets;
        const end = sample.words[@as(usize, @intCast(span.end)) - 1].source orelse return error.InvalidBoundaryTrainingTargets;
        return sample.original_text[start.start..end.end];
    }
    fn value(self: *Compiler, sample: processor.Sample, s: usize, f: usize, definition: schema_mod.Field, annotation: Value, mapping: offsets_mod.OffsetMap, words: []const DocumentWord) !RecordValue {
        switch (annotation) {
            .choice => |choice| {
                if (choice >= definition.choices.len) return error.InvalidBoundaryTrainingTargets;
                try self.validators(definition.validators, definition.choices[choice]);
                var position: ?usize = null;
                for (sample.enum_choices) |route_| {
                    try self.tick();
                    if (route_.structure_index == s and route_.field_index == f and route_.choice_index == choice) {
                        if (position != null or route_.score_start >= sample.prefix_word_count) return error.InvalidBoundaryTrainingRouting;
                        position = route_.score_start;
                    }
                }
                const start = position orelse return error.InvalidBoundaryTrainingRouting;
                const alternatives = try self.allocator.alloc(Span, 1);
                alternatives[0] = .{ .start = @intCast(start), .end = @intCast(start + 1) };
                return .{ .alternatives = alternatives, .choice = choice };
            },
            .document => |sources| {
                if (sources.len == 0) return error.InvalidBoundaryTrainingTargets;
                try self.charge(sources.len);
                const alternatives = try self.allocator.alloc(Span, sources.len);
                var choice: ?usize = null;
                for (sources, alternatives, 0..) |source, *out, index| {
                    out.* = try self.mapSource(source, mapping, words);
                    for (alternatives[0..index]) |previous| {
                        try self.tick();
                        if (sameSpan(previous, out.*)) return error.DuplicateBoundaryTrainingTarget;
                    }
                    const text = try self.surface(sample, out.*);
                    if (definition.choices.len > 0) {
                        var selected: ?usize = null;
                        for (definition.choices, 0..) |candidate, c| {
                            try self.tick();
                            if (try sameLiteral(self, text, candidate)) {
                                if (selected != null) return error.AmbiguousBoundaryTrainingChoice;
                                selected = c;
                            }
                        }
                        const current = selected orelse return error.InvalidBoundaryTrainingTargets;
                        if (choice != null and current != choice.?) return error.InvalidBoundaryTrainingTargets;
                        choice = current;
                        try self.validators(definition.validators, definition.choices[current]);
                    } else try self.validators(definition.validators, text);
                }
                return .{ .alternatives = alternatives, .choice = choice };
            },
        }
    }
    fn endpoint(self: *Compiler, endpoint_: Endpoint, entity_spans: []const Span, mapping: offsets_mod.OffsetMap, words: []const DocumentWord) !Span {
        return switch (endpoint_) {
            .document => |source| self.mapSource(source, mapping, words),
            .entity => |index| if (index < entity_spans.len) entity_spans[index] else error.InvalidBoundaryTrainingTargets,
        };
    }
};
fn sameSpan(a: Span, b: Span) bool {
    return a.start == b.start and a.end == b.end;
}
fn sameLiteral(compiler: *Compiler, a: []const u8, b: []const u8) !bool {
    var ai = (try std.unicode.Utf8View.init(a)).iterator();
    var bi = (try std.unicode.Utf8View.init(b)).iterator();
    while (ai.nextCodepoint()) |cp| {
        try compiler.tick();
        const other = bi.nextCodepoint() orelse return false;
        if (!unicode.regexCaseEquivalent(cp, other)) return false;
    }
    return bi.nextCodepoint() == null;
}
fn contains(values: []const usize, value: usize) bool {
    for (values) |candidate| if (candidate == value) return true;
    return false;
}
fn checkedElements(a: usize, b: usize, limit: usize) !usize {
    const size = std.math.mul(usize, a, b) catch return error.BoundaryTrainingLimitExceeded;
    if (size > limit) return error.BoundaryTrainingLimitExceeded;
    return size;
}

/// Samples are borrowed from processor.PreparedBatch.samples. Schema objects
/// must have the same fingerprints and order. The returned target batch owns
/// its strings, identities, arrays, and hashes independently of every input.
pub fn compileBatch(a: Allocator, samples: []const processor.Sample, schemas: []const *const schema_mod.CompiledSchema, annotations: []const Annotations, options: Options) !Targets {
    if (options.control) |control| try control.check();
    if (samples.len == 0 or samples.len != schemas.len or samples.len != annotations.len) return error.InvalidBoundaryTrainingShape;
    if (samples.len > options.max_batch or options.max_work == 0) return error.BoundaryTrainingLimitExceeded;
    if (options.max_gold_per_query == 0 or (options.gold_capacity != null and (options.gold_capacity.? == 0 or options.gold_capacity.? > options.max_gold_per_query)))
        return error.InvalidBoundaryTrainingOptions;
    var word_width: usize = 0;
    var query_width: usize = 0;
    var classification_width: usize = 0;
    var total_text: usize = 0;
    for (samples, schemas, annotations) |sample, compiled, annotation| {
        if (!std.mem.eql(u8, &sample.schema_fingerprint, &compiled.fingerprint) or !std.mem.eql(u8, &annotation.schema_fingerprint, &compiled.fingerprint))
            return error.BoundaryTrainingSchemaMismatch;
        if (sample.original_text.len > options.max_text_bytes or sample.original_text.len > options.max_total_text_bytes -| total_text or sample.words.len > options.max_words or sample.queries.len > options.max_queries or sample.classification_labels.len > options.max_classification_labels)
            return error.BoundaryTrainingLimitExceeded;
        total_text += sample.original_text.len;
        word_width = @max(word_width, sample.words.len);
        query_width = @max(query_width, sample.queries.len);
        classification_width = @max(classification_width, sample.classification_labels.len);
    }
    const query_count = try checkedElements(samples.len, query_width, options.max_padded_elements);
    const classification_count = try checkedElements(samples.len, classification_width, options.max_padded_elements);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var compiler = Compiler{ .allocator = owned, .scratch = a, .options = options };
    const output = try owned.alloc(Sample, samples.len);
    const query_mask = try owned.alloc(bool, query_count);
    const classification_targets = try owned.alloc(f32, classification_count);
    const classification_mask = try owned.alloc(bool, classification_count);
    @memset(query_mask, false);
    @memset(classification_targets, 0);
    @memset(classification_mask, false);
    var observed_gold: usize = 0;
    for (samples, schemas, annotations, output, 0..) |sample, compiled, annotation, *out, b| {
        try compiler.progress(b, null, "routing");
        const schema = compiled.schema;
        try compiler.validateRoutes(sample, schema);
        if (schema.joint_ie != null and (annotation.entities.len > options.joint_limits.max_nodes or annotation.relations.len > options.joint_limits.max_edges))
            return error.BoundaryTrainingTargetCapacityExceeded;
        if (sample.queries.len == 0 and annotation.classifications.len == 0) return error.UnsupervisedBoundaryTrainingSample;
        var mapping = try offsets_mod.OffsetMap.init(a, sample.original_text, options.max_text_bytes);
        defer mapping.deinit();
        const words = try compiler.documentWords(sample);
        defer a.free(words);
        const mentions = try owned.alloc(std.ArrayListUnmanaged(Span), sample.queries.len);
        for (mentions) |*list| list.* = .empty;
        @memset(query_mask[b * query_width ..][0..sample.queries.len], true);
        try compiler.charge(annotation.entities.len);
        const entity_spans = try owned.alloc(Span, annotation.entities.len);
        const entity_count = if (schema.joint_ie) |j| j.entities.len else schema.entities.len;
        for (annotation.entities, entity_spans, 0..) |entity, *span, e| {
            try compiler.progress(b, e, "entities");
            if (entity.entity_type >= entity_count) return error.InvalidBoundaryTrainingTargets;
            span.* = try compiler.mapSource(entity.source, mapping, words);
            for (annotation.entities[0..e], entity_spans[0..e]) |previous, previous_span| {
                try compiler.tick();
                if (previous.entity_type == entity.entity_type and sameSpan(previous_span, span.*)) return error.DuplicateBoundaryTrainingTarget;
            }
            if (schema.joint_ie == null) try compiler.validators(schema.entities[entity.entity_type].validators, try compiler.surface(sample, span.*));
            try compiler.addMention(mentions, try compiler.route(sample, .entity, entity.entity_type, null), span.*);
            try compiler.charge(entity.attributes.len);
            for (entity.attributes, 0..) |attribute, index| {
                if (attribute.group >= schema.entity_attributes.len) return error.InvalidBoundaryTrainingTargets;
                for (entity.attributes[0..index]) |previous| if (previous.group == attribute.group) return error.DuplicateBoundaryTrainingTarget;
                const group = schema.entity_attributes[attribute.group];
                if (group.applies_to) |types| if (!contains(types, entity.entity_type)) return error.InvalidBoundaryTrainingTargets;
                if (!group.multi_label and attribute.labels.len != 1) return error.InvalidBoundaryTrainingTargets;
                try compiler.charge(attribute.labels.len);
                for (attribute.labels, 0..) |label, index_| {
                    if (label >= group.labels.len or contains(attribute.labels[0..index_], label)) return error.InvalidBoundaryTrainingTargets;
                    try compiler.addMention(mentions, try compiler.route(sample, .attribute, attribute.group, label), span.*);
                }
            }
            for (schema.entity_attributes, 0..) |group, g| {
                if (group.applies_to) |types| if (!contains(types, entity.entity_type)) continue;
                var present = false;
                for (entity.attributes) |attribute| if (attribute.group == g) {
                    present = true;
                    break;
                };
                if (!present) return error.IncompleteBoundaryAttributeSupervision;
            }
        }
        try compiler.charge(annotation.classifications.len);
        const selected = try owned.alloc(constraints.Selection, schema.classifications.len);
        const possible = try owned.alloc(constraints.Selection, schema.classifications.len);
        const classified = try owned.alloc(bool, schema.classifications.len);
        @memset(selected, 0);
        @memset(possible, 0);
        @memset(classified, false);
        for (annotation.classifications, 0..) |classification, index| {
            try compiler.progress(b, index, "classifications");
            if (classification.task >= schema.classifications.len or classified[classification.task]) return error.InvalidBoundaryTrainingTargets;
            classified[classification.task] = true;
            const task = schema.classifications[classification.task].task;
            if (classification.labels.len < task.min_labels or classification.labels.len > task.maximum()) return error.InvalidBoundaryTrainingTargets;
            try compiler.charge(classification.labels.len);
            for (classification.labels, 0..) |label, j| {
                if (label >= task.labels.len or contains(classification.labels[0..j], label)) return error.InvalidBoundaryTrainingTargets;
                selected[classification.task] |= @as(constraints.Selection, 1) << @as(std.math.Log2Int(constraints.Selection), @intCast(label));
            }
            for (task.labels, 0..) |_, label| {
                const index_ = b * classification_width + try compiler.classRoute(sample, classification.task, label);
                classification_mask[index_] = true;
                classification_targets[index_] = if (contains(classification.labels, label)) 1 else 0;
            }
        }
        if (schema.classification_constraints.roots.len > 0) {
            for (classified) |present| if (!present) return error.IncompleteBoundaryClassificationSupervision;
            if (try schema.classification_constraints.evaluate(.{ .selected = selected, .possible = possible }) != .yes) return error.InvalidBoundaryTrainingTargets;
        }
        try compiler.charge(annotation.records.len);
        const records = try owned.alloc(RecordTarget, annotation.records.len);
        for (annotation.records, records, 0..) |record, *target, index| {
            try compiler.progress(b, index, "records");
            if (record.structure >= schema.structures.len or record.id.len == 0 or !std.unicode.utf8ValidateSlice(record.id)) return error.InvalidBoundaryTrainingTargets;
            if (record.id.len > options.max_record_id_bytes) return error.BoundaryTrainingLimitExceeded;
            for (annotation.records[0..index]) |previous| {
                try compiler.tick();
                if (previous.structure == record.structure and std.mem.eql(u8, previous.id, record.id)) return error.DuplicateBoundaryTrainingTarget;
            }
            const structure = schema.structures[record.structure];
            const fields = try owned.alloc(RecordField, structure.fields.len);
            const seen = try owned.alloc(bool, structure.fields.len);
            @memset(seen, false);
            for (structure.fields, fields, 0..) |_, *field, f| field.* = .{ .field = f, .query = try compiler.route(sample, .field, record.structure, f), .values = &.{} };
            try compiler.charge(record.fields.len);
            for (record.fields) |field| {
                if (field.field >= structure.fields.len or seen[field.field]) return error.InvalidBoundaryTrainingTargets;
                seen[field.field] = true;
                const definition = structure.fields[field.field];
                const cardinality = fieldCardinality(definition, structure.anchor != null and structure.anchor.? == field.field);
                if ((cardinality == .required_one or cardinality == .optional_one) and field.values.len > 1) return error.InvalidBoundaryTrainingTargets;
                try compiler.charge(field.values.len);
                const values = try owned.alloc(RecordValue, field.values.len);
                for (field.values, values, 0..) |value_, *v, value_index| {
                    v.* = try compiler.value(sample, record.structure, field.field, definition, value_, mapping, words);
                    for (values[0..value_index]) |previous| {
                        if (v.choice != null and previous.choice == v.choice) return error.DuplicateBoundaryTrainingTarget;
                        for (previous.alternatives) |old_span| for (v.alternatives) |span| {
                            try compiler.tick();
                            if (sameSpan(old_span, span)) return error.DuplicateBoundaryTrainingTarget;
                        };
                    }
                    for (v.alternatives) |span| try compiler.addMention(mentions, fields[field.field].query, span);
                }
                fields[field.field].values = values;
            }
            for (structure.fields, fields, 0..) |definition, field, f| {
                const card = fieldCardinality(definition, structure.anchor != null and structure.anchor.? == f);
                const required = card == .required_one or card == .one_or_more;
                if (required and field.values.len == 0) return error.MissingBoundaryTrainingRequiredField;
            }
            target.* = .{ .structure = record.structure, .group = sample.queries[fields[0].query].group_index, .id = try owned.dupe(u8, record.id), .anchor_query = if (structure.anchor) |anchor| fields[anchor].query else null, .fields = fields };
        }
        try compiler.charge(annotation.relations.len);
        const relations = try owned.alloc(RelationTarget, annotation.relations.len);
        const relation_count = if (schema.joint_ie) |j| j.relations.len else schema.relations.len;
        for (annotation.relations, relations, 0..) |relation, *target, index| {
            try compiler.progress(b, index, "relations");
            if (relation.relation_type >= relation_count) return error.InvalidBoundaryTrainingTargets;
            if (schema.joint_ie != null and (relation.head != .entity or relation.tail != .entity)) return error.InvalidBoundaryTrainingTargets;
            const head = try compiler.endpoint(relation.head, entity_spans, mapping, words);
            const tail = try compiler.endpoint(relation.tail, entity_spans, mapping, words);
            for (relations[0..index]) |previous| {
                try compiler.tick();
                const same_identity = schema.joint_ie == null or (previous.head_entity == relation.head.entity and previous.tail_entity == relation.tail.entity);
                if (same_identity and previous.relation_type == relation.relation_type and sameSpan(previous.head, head) and sameSpan(previous.tail, tail)) return error.DuplicateBoundaryTrainingTarget;
            }
            const hq = try compiler.route(sample, .relation_head, relation.relation_type, null);
            const tq = try compiler.route(sample, .relation_tail, relation.relation_type, null);
            try compiler.addMention(mentions, hq, head);
            try compiler.addMention(mentions, tq, tail);
            target.* = .{ .relation_type = relation.relation_type, .head_query = hq, .tail_query = tq, .head = head, .tail = tail, .head_entity = if (relation.head == .entity) relation.head.entity else null, .tail_entity = if (relation.tail == .entity) relation.tail.entity else null };
        }
        if (schema.joint_ie) |j| {
            const nodes = try a.alloc(joint.Node, annotation.entities.len);
            defer a.free(nodes);
            const edges = try a.alloc(joint.Edge, relations.len);
            defer a.free(edges);
            for (annotation.entities, entity_spans, nodes) |entity, span, *node| node.* = .{ .entity_type = entity.entity_type, .start = @intCast(span.start), .end = @intCast(span.end), .probability = 1, .utility = 0, .required = true };
            for (relations, edges) |relation, *edge| edge.* = .{ .relation_type = relation.relation_type, .head = relation.head_entity.?, .tail = relation.tail_entity.?, .probability = 1, .utility = 0, .required = true };
            var limits = options.joint_limits;
            limits.control = options.control;
            var checked = try joint.decode(a, j, nodes, edges, limits);
            defer checked.deinit();
            if (checked.exhausted) return error.JointSearchExhausted;
            if (!checked.valid()) return error.InvalidBoundaryTrainingTargets;
        }
        const grouped = try owned.alloc([]const Span, mentions.len);
        for (mentions, grouped) |*list, *spans| {
            spans.* = try list.toOwnedSlice(owned);
            observed_gold = @max(observed_gold, spans.len);
        }
        var text_hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(sample.original_text, &text_hash, .{});
        out.* = .{ .schema_fingerprint = compiled.fingerprint, .text_fingerprint = text_hash, .word_count = sample.words.len, .mentions = grouped, .entity_spans = entity_spans, .records = records, .relations = relations };
    }
    const capacity = options.gold_capacity orelse @max(observed_gold, 1);
    if (observed_gold > capacity) return error.BoundaryTrainingTargetCapacityExceeded;
    const padded_count = try checkedElements(query_count, capacity, options.max_padded_elements);
    const pairs = try owned.alloc(Span, padded_count);
    const mask = try owned.alloc(bool, padded_count);
    @memset(pairs, .{ .start = 0, .end = 0 });
    @memset(mask, false);
    for (output, 0..) |sample, b| for (sample.mentions, 0..) |mentions, q| {
        try compiler.tick();
        const row = (b * query_width + q) * capacity;
        @memcpy(pairs[row..][0..mentions.len], mentions);
        @memset(mask[row..][0..mentions.len], true);
    };
    try compiler.check();
    return .{ .arena = arena, .samples = output, .word_width = word_width, .query_width = query_width, .classification_width = classification_width, .gold_capacity = capacity, .mention_pairs = pairs, .mention_mask = mask, .query_mask = query_mask, .classification_targets = classification_targets, .classification_mask = classification_mask, .work = compiler.work };
}

const TestTokenizer = struct {
    const markers = [_][]const u8{ "[P]", "[C]", "[E]", "[R]", "[L]", "[SEP_STRUCT]", "[SEP_TEXT]", "[DESCRIPTION]", "[EXAMPLE]", "[OUTPUT]" };
    state: u8 = 0,
    fn tokenizer(self: *TestTokenizer) @import("inference_tokenizer").Tokenizer {
        return .{ .ptr = self, .vtable = &.{ .encode = encode, .encodeInto = encodeInto, .encodeForModel = undefined, .encodeGeneration = undefined, .decode = undefined, .specialTokens = specialTokens, .allSpecialTokenIds = specialIds, .vocabSize = vocabSize, .deinit = undefined } };
    }
    fn encode(raw: *anyopaque, a: Allocator, text: []const u8) ![]i32 {
        var output = std.ArrayListUnmanaged(i32).empty;
        errdefer output.deinit(a);
        try encodeInto(raw, a, text, &output);
        return output.toOwnedSlice(a);
    }
    fn encodeInto(_: *anyopaque, a: Allocator, text: []const u8, output: *std.ArrayListUnmanaged(i32)) !void {
        var pos: usize = 0;
        while (pos < text.len) {
            var matched = false;
            for (markers, 0..) |marker, i| if (std.mem.startsWith(u8, text[pos..], marker)) {
                try output.append(a, @as(i32, @intCast(300 + i)));
                pos += marker.len;
                matched = true;
                break;
            };
            if (!matched) {
                try output.append(a, @as(i32, text[pos]) + 10);
                pos += 1;
            }
        }
    }
    fn specialTokens(_: *anyopaque) @import("inference_tokenizer").SpecialTokens {
        return .{ .unk_id = 1 };
    }
    fn specialIds(_: *anyopaque, a: Allocator) ![]u32 {
        const output = try a.alloc(u32, markers.len);
        for (output, 0..) |*value_, i| value_.* = @intCast(300 + i);
        return output;
    }
    fn vocabSize(_: *anyopaque) usize {
        return 512;
    }
};
const rich_schema =
    \\{"entities":["person","company"],"entity_attributes":{"tone":{"labels":["positive","negative"],"applies_to":["person"]}},"classifications":[{"name":"topics","labels":["meeting","billing"],"mode":"multi"}],"structures":{"deal":{"mode":"natural","anchor":"party","fields":{"party":{"dtype":"str"},"state":{"dtype":"str","choices":["paid","due"]}}}},"relations":[{"type":"met"}]}
;
const rich_entities = [_]Entity{
    .{ .entity_type = 0, .source = .{ .start = 0, .end = 3 }, .attributes = &.{.{ .group = 0, .labels = &.{0} }} },
    .{ .entity_type = 1, .source = .{ .start = 8, .end = 12 } },
    .{ .entity_type = 0, .source = .{ .start = 14, .end = 17 }, .attributes = &.{.{ .group = 0, .labels = &.{1} }} },
};
const rich_records = [_]Record{
    .{ .structure = 0, .id = "deal:first", .fields = &.{
        .{ .field = 0, .values = &.{.{ .document = &.{.{ .start = 0, .end = 3 }} }} },
        .{ .field = 1, .values = &.{.{ .choice = 0 }} },
    } },
    .{ .structure = 0, .id = "deal:second", .fields = &.{
        .{ .field = 0, .values = &.{.{ .document = &.{.{ .start = 14, .end = 17 }} }} },
        .{ .field = 1, .values = &.{.{ .choice = 1 }} },
    } },
};
test "boundary training targets preserve mixed schema identities attributes enums and ragged masks" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, rich_schema, .{});
    defer compiled.deinit();
    var other = try schema_mod.compile(a, "{\"entities\":[\"person\"]}", .{});
    defer other.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{ .{ .text = "Ada met Acme. Ada paid €12.", .schema = &compiled }, .{ .text = "Bob.", .schema = &other } }, .{});
    defer prepared.deinit();
    var targets = try compileBatch(a, prepared.samples, &.{ &compiled, &other }, &.{
        .{ .schema_fingerprint = compiled.fingerprint, .entities = &rich_entities, .records = &rich_records, .classifications = &.{.{ .task = 0, .labels = &.{1} }}, .relations = &.{.{ .relation_type = 0, .head = .{ .entity = 0 }, .tail = .{ .entity = 1 } }} },
        .{ .schema_fingerprint = other.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 0, .end = 3 } }} },
    }, .{});
    defer targets.deinit();
    try std.testing.expectEqual(@as(usize, 2), targets.samples.len);
    try std.testing.expectEqual(@as(usize, 2), targets.gold_capacity);
    try std.testing.expectEqual(prepared.word_width, targets.word_width);
    try std.testing.expectEqual(prepared.query_width, targets.query_width);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 0, 0 }, targets.classification_targets);
    try std.testing.expectEqualSlices(bool, &.{ true, true, false, false }, targets.classification_mask);
    try std.testing.expect(targets.query_mask[targets.query_width]);
    for (targets.query_mask[targets.query_width + 1 ..]) |mask| try std.testing.expect(!mask);
    const first = targets.samples[0];
    try std.testing.expectEqualStrings("deal:first", first.records[0].id);
    try std.testing.expectEqualStrings("deal:second", first.records[1].id);
    try std.testing.expectEqual(@as(?usize, 0), first.records[0].fields[1].values[0].choice);
    try std.testing.expect(first.records[0].fields[1].values[0].alternatives[0].start < prepared.samples[0].prefix_word_count);
    try std.testing.expect(!sameSpan(first.records[0].fields[0].values[0].alternatives[0], first.records[1].fields[0].values[0].alternatives[0]));
    try std.testing.expectEqual(@as(?usize, 0), first.relations[0].head_entity);
    try std.testing.expectEqual(@as(?usize, 1), first.relations[0].tail_entity);
    for (prepared.samples[0].queries, 0..) |query, q| if (query.kind == .attribute) {
        try std.testing.expectEqual(@as(usize, 1), first.mentions[q].len);
        try std.testing.expect(sameSpan(first.mentions[q][0], first.entity_spans[if (query.label_index == 0) 0 else 2]));
    };
    var dense = try loss.denseTargets(a, targets.gold(), targets.word_width, .{});
    defer dense.deinit();
    try std.testing.expectEqual(targets.samples.len * targets.query_width * (targets.word_width + 1), dense.starts.len);
}

test "boundary training targets keep explicit alternative occurrences and reject hidden gold truncation" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"structures\":{\"event\":{\"mode\":\"latent\",\"fields\":{\"actor\":{\"dtype\":\"str\",\"cardinality\":\"required_one\"}}}}}", .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Ada", .schema = &compiled }}, .{});
    defer prepared.deinit();
    const records = [_]Record{.{ .structure = 0, .id = "one-identity", .fields = &.{.{ .field = 0, .values = &.{.{ .document = &.{ .{ .start = 0, .end = 3 }, .{ .start = 4, .end = 7 } } }} }} }};
    const annotations = [_]Annotations{.{ .schema_fingerprint = compiled.fingerprint, .records = &records }};
    var targets = try compileBatch(a, prepared.samples, &.{&compiled}, &annotations, .{});
    defer targets.deinit();
    try std.testing.expectEqual(@as(usize, 1), targets.samples[0].records.len);
    try std.testing.expectEqual(@as(usize, 1), targets.samples[0].records[0].fields[0].values.len);
    try std.testing.expectEqual(@as(usize, 2), targets.samples[0].records[0].fields[0].values[0].alternatives.len);
    try std.testing.expectEqual(@as(usize, 2), targets.samples[0].mentions[0].len);
    try std.testing.expectError(error.BoundaryTrainingTargetCapacityExceeded, compileBatch(a, prepared.samples, &.{&compiled}, &annotations, .{ .gold_capacity = 1 }));
    try std.testing.expectError(error.BoundaryTrainingTargetCapacityExceeded, compileBatch(a, prepared.samples, &.{&compiled}, &annotations, .{ .max_gold_per_query = 1 }));
    try std.testing.expectError(error.MissingBoundaryTrainingRequiredField, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .records = &.{.{ .structure = 0, .id = "missing", .fields = &.{} }} }}, .{}));
}

test "boundary training targets default string fields to one distinct value" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"structures\":{\"event\":{\"mode\":\"latent\",\"fields\":{\"actor\":\"str\"}}}}", .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Bob", .schema = &compiled }}, .{});
    defer prepared.deinit();
    const distinct = [_]Value{ .{ .document = &.{.{ .start = 0, .end = 3 }} }, .{ .document = &.{.{ .start = 4, .end = 7 }} } };
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .records = &.{.{ .structure = 0, .id = "one-record", .fields = &.{.{ .field = 0, .values = &distinct }} }} }}, .{}));
    var empty = try compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .records = &.{.{ .structure = 0, .id = "absent-optional", .fields = &.{} }} }}, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.samples[0].records[0].fields[0].values.len);
}

test "boundary training targets require exact original Unicode boundaries and complete attribute labels" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"entities\":[\"word\"],\"entity_attributes\":{\"status\":{\"labels\":[\"yes\",\"no\"]}}}", .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "😀Ada café", .schema = &compiled }}, .{});
    defer prepared.deinit();
    const attributes = [_]Attribute{.{ .group = 0, .labels = &.{0} }};
    var targets = try compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 6, .end = 10, .unit = .utf16_codeunits }, .attributes = &attributes }} }}, .{});
    defer targets.deinit();
    const span = targets.samples[0].entity_spans[0];
    try std.testing.expectEqual(@as(usize, 8), prepared.samples[0].words[@intCast(span.start)].source.?.start);
    try std.testing.expectEqual(@as(usize, 13), prepared.samples[0].words[@as(usize, @intCast(span.end)) - 1].source.?.end);
    try std.testing.expectError(error.UnalignedBoundaryTrainingTarget, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 9, .end = 13 }, .attributes = &attributes }} }}, .{}));
    try std.testing.expectError(error.InvalidUtf16Boundary, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 1, .end = 2, .unit = .utf16_codeunits }, .attributes = &attributes }} }}, .{}));
    try std.testing.expectError(error.IncompleteBoundaryAttributeSupervision, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 8, .end = 13 } }} }}, .{}));
    var wrong = compiled.fingerprint;
    wrong[0] ^= 1;
    try std.testing.expectError(error.BoundaryTrainingSchemaMismatch, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = wrong }}, .{}));
}

test "boundary training targets validate typed JointIE gold graph without relabeling identities" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a,
        \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"],"max_per_head":1}}}}
    , .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Acme Beta", .schema = &compiled }}, .{});
    defer prepared.deinit();
    const entities = [_]Entity{
        .{ .entity_type = 0, .source = .{ .start = 0, .end = 3 } },
        .{ .entity_type = 1, .source = .{ .start = 4, .end = 8 } },
        .{ .entity_type = 1, .source = .{ .start = 9, .end = 13 } },
    };
    const relation = Relation{ .relation_type = 0, .head = .{ .entity = 0 }, .tail = .{ .entity = 1 } };
    var targets = try compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &entities, .relations = &.{relation} }}, .{});
    defer targets.deinit();
    try std.testing.expectEqual(@as(usize, 1), targets.samples[0].relations.len);
    try std.testing.expectEqual(@as(?usize, 1), targets.samples[0].relations[0].tail_entity);
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &entities, .relations = &.{ relation, .{ .relation_type = 0, .head = .{ .entity = 0 }, .tail = .{ .entity = 2 } } } }}, .{}));
    try std.testing.expectError(error.InvalidBoundaryTrainingTargets, compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &entities, .relations = &.{.{ .relation_type = 0, .head = .{ .entity = 1 }, .tail = .{ .entity = 0 } }} }}, .{}));
}

fn allocationLifecycle(a: Allocator) !void {
    var compiled = try schema_mod.compile(a, rich_schema, .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada met Acme. Ada paid €12.", .schema = &compiled }}, .{});
    defer prepared.deinit();
    var targets = try compileBatch(a, prepared.samples, &.{&compiled}, &.{.{ .schema_fingerprint = compiled.fingerprint, .entities = &rich_entities, .records = &rich_records, .classifications = &.{.{ .task = 0, .labels = &.{} }}, .relations = &.{.{ .relation_type = 0, .head = .{ .entity = 0 }, .tail = .{ .entity = 1 } }} }}, .{});
    defer targets.deinit();
    try std.testing.expectEqual(@as(usize, 2), targets.samples[0].records.len);
}
test "boundary training targets release all allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}

test "boundary training target admission and cancellation fail atomically" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"entities\":[\"word\"]}", .{});
    defer compiled.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Ada", .schema = &compiled }}, .{});
    defer prepared.deinit();
    const annotations = [_]Annotations{.{ .schema_fingerprint = compiled.fingerprint, .entities = &.{.{ .entity_type = 0, .source = .{ .start = 0, .end = 3 } }} }};
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.Cancelled, compileBatch(failing.allocator(), prepared.samples, &.{&compiled}, &annotations, .{ .control = .{ .check_fn = Cancel.check } }));
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, compileBatch(failing.allocator(), prepared.samples, &.{&compiled}, &annotations, .{ .max_work = 0 }));
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, compileBatch(failing.allocator(), prepared.samples, &.{&compiled}, &annotations, .{ .max_text_bytes = 1 }));
    try std.testing.expect(!failing.has_induced_failure);
    var failure = Failure{};
    try std.testing.expectError(error.BoundaryTrainingLimitExceeded, compileBatch(a, prepared.samples, &.{&compiled}, &annotations, .{ .max_work = 1, .failure = &failure }));
    try std.testing.expectEqual(@as(?usize, 0), failure.sample);
}

test "boundary training targets match pinned schema preprocessing and target packing" {
    const ExpectedField = struct { query: usize, values: []const []const Span };
    const ExpectedRecord = struct { id: []const u8, group: usize, anchor_query: ?usize, fields: []const ExpectedField };
    const Expected = struct {
        word_width: usize,
        query_width: usize,
        classification_width: usize,
        gold_capacity: usize,
        mention_pairs: []const Span,
        mention_mask: []const bool,
        query_mask: []const bool,
        classification_targets: []const f32,
        classification_mask: []const bool,
        records: []const []const ExpectedRecord,
    };
    const Fixture = struct {
        format_version: u32,
        provenance: struct { upstream_commit: []const u8 },
        cases: []const struct {
            id: []const u8,
            word_splitter: processor.WordSplitter = .whitespace,
            capacity: ?usize = null,
            capacity_error: bool = false,
            samples: []const struct { text: []const u8, schema: std.json.Value, annotations: Annotations },
            upstream_word_tokens: []const []const []const u8,
            upstream_prefix_counts: []const usize,
            expected: ?Expected = null,
        },
    };
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_targets.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(Fixture, a, bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u32, 1), fixture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", fixture.value.provenance.upstream_commit);
    for (fixture.value.cases) |case| {
        errdefer std.debug.print("training target fixture: {s}\n", .{case.id});
        const compiled = try a.alloc(schema_mod.CompiledSchema, case.samples.len);
        defer a.free(compiled);
        var count: usize = 0;
        defer for (compiled[0..count]) |*schema| schema.deinit();
        const schemas = try a.alloc(*const schema_mod.CompiledSchema, case.samples.len);
        defer a.free(schemas);
        const items = try a.alloc(processor.Item, case.samples.len);
        defer a.free(items);
        const annotations = try a.alloc(Annotations, case.samples.len);
        defer a.free(annotations);
        for (case.samples, compiled, schemas, items, annotations) |sample, *schema, *pointer, *item, *annotation| {
            const json = try std.json.Stringify.valueAlloc(a, sample.schema, .{});
            defer a.free(json);
            schema.* = try schema_mod.compile(a, json, .{});
            count += 1;
            pointer.* = schema;
            item.* = .{ .text = sample.text, .schema = schema };
            annotation.* = sample.annotations;
            annotation.schema_fingerprint = schema.fingerprint;
        }
        var tokenizer = TestTokenizer{};
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), items, .{ .word_splitter = case.word_splitter });
        defer prepared.deinit();
        for (prepared.samples, case.upstream_word_tokens, case.upstream_prefix_counts) |sample, tokens, prefix_count| {
            try std.testing.expectEqual(prefix_count, sample.prefix_word_count);
            try std.testing.expectEqual(tokens.len, sample.words.len);
            for (tokens, sample.words) |token, word| try std.testing.expectEqualStrings(token, word.text);
        }
        if (case.capacity_error) {
            try std.testing.expectError(error.BoundaryTrainingTargetCapacityExceeded, compileBatch(a, prepared.samples, schemas, annotations, .{ .gold_capacity = case.capacity }));
            continue;
        }
        var targets = try compileBatch(a, prepared.samples, schemas, annotations, .{ .gold_capacity = case.capacity });
        defer targets.deinit();
        const expected = case.expected.?;
        try std.testing.expectEqual(expected.word_width, targets.word_width);
        try std.testing.expectEqual(expected.query_width, targets.query_width);
        try std.testing.expectEqual(expected.classification_width, targets.classification_width);
        try std.testing.expectEqual(expected.gold_capacity, targets.gold_capacity);
        try std.testing.expectEqualDeep(expected.mention_pairs, targets.mention_pairs);
        try std.testing.expectEqualSlices(bool, expected.mention_mask, targets.mention_mask);
        try std.testing.expectEqualSlices(bool, expected.query_mask, targets.query_mask);
        try std.testing.expectEqualSlices(f32, expected.classification_targets, targets.classification_targets);
        try std.testing.expectEqualSlices(bool, expected.classification_mask, targets.classification_mask);
        for (expected.records, targets.samples) |records, sample| {
            try std.testing.expectEqual(records.len, sample.records.len);
            for (records, sample.records) |want, got| {
                try std.testing.expectEqualStrings(want.id, got.id);
                try std.testing.expectEqual(want.group, got.group);
                try std.testing.expectEqual(want.anchor_query, got.anchor_query);
                try std.testing.expectEqual(want.fields.len, got.fields.len);
                for (want.fields, got.fields) |want_field, got_field| {
                    try std.testing.expectEqual(want_field.query, got_field.query);
                    try std.testing.expectEqual(want_field.values.len, got_field.values.len);
                    for (want_field.values, got_field.values) |alternatives, value| try std.testing.expectEqualDeep(alternatives, value.alternatives);
                }
            }
        }
    }
}
