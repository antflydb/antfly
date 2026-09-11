// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-side inputs to the closed runtime policy. This module derives
//! features from compiled schemas and geometry from real preparation/planning.
//! It neither loads release evidence nor changes inference semantics.
const std = @import("std");
const policy = @import("../models/gliner_boundary_qualification.zig");
const artifact = @import("../models/gliner_boundary_bundle.zig");
const model = @import("../models/gliner_boundary.zig");
const wire = @import("extraction_v2.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const document = @import("../pipelines/gliner_boundary_long_document.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

pub fn requiredFeatures(request: *const wire.Request, control: ?Control) !policy.Features {
    try check(control);
    if (request.items.len == 0) return error.InvalidExtractionRequest;
    var result = policy.Features.initEmpty();
    for (request.items) |item| {
        try check(control);
        const schema = item.compiled.schema;
        if (schema.entities.len != 0) result.insert(.entities);
        if (schema.entity_attributes.len != 0) result.insert(.entity_attributes);
        for (schema.entities) |entity| {
            try check(control);
            if (entity.description != null) result.insert(.schema_descriptions);
            if (entity.validators.len != 0) result.insert(.regex_validation);
        }
        for (schema.classifications) |task| {
            try check(control);
            result.insert(switch (task.mode) {
                .single => .classification_single,
                .multi => .classification_multi,
                .ordinal => .classification_ordinal,
            });
            if (task.structured_selection) result.insert(.classification_structured);
            if (task.prompt != null or task.hypothesis_template != null or task.examples.len != 0) result.insert(.classification_context);
            for (task.label_definitions) |label| {
                try check(control);
                if (label.description != null) result.insert(.schema_descriptions);
            }
        }
        if (schema.classification_constraints.roots.len != 0) {
            result.insert(.classification_constraints);
            // The pipeline enables structured selection for the entire
            // constraint program, including tasks without explicit set knobs.
            result.insert(.classification_structured);
        }
        for (schema.structures) |structure| {
            try check(control);
            result.insert(if (structure.mode) |mode| switch (mode) {
                .natural => .records_natural,
                .latent => .records_latent,
                .anchorless => .records_anchorless,
            } else .legacy_structures);
            if (structure.occurrence_policy != null) result.insert(.record_occurrence_policy);
            for (structure.fields) |field| {
                try check(control);
                if (field.choices.len != 0) result.insert(.field_choices);
                if (field.dtype == .list or field.cardinality != null or field.exclusive) result.insert(.field_rules);
                if (field.description != null) result.insert(.schema_descriptions);
                if (field.validators.len != 0) result.insert(.regex_validation);
            }
        }
        if (schema.relations.len != 0) result.insert(.relations);
        for (schema.relations) |relation| {
            try check(control);
            if (relation.source != null or relation.target != null) result.insert(.relation_endpoints);
            if (relation.description != null) result.insert(.schema_descriptions);
        }
        if (schema.joint_ie) |joint| {
            result.insert(.joint_ie);
            if (joint.constraints.len != 0) result.insert(.joint_constraints);
            for (joint.entities) |entity| {
                try check(control);
                if (entity.description != null) result.insert(.schema_descriptions);
            }
            for (joint.relations) |relation| {
                try check(control);
                if (relation.description != null) result.insert(.schema_descriptions);
            }
        }
        const options = item.options;
        result.insert(switch (options.word_splitter) {
            .whitespace => .word_whitespace,
            .char => .word_char,
        });
        result.insert(switch (options.overlap) {
            .flat => .overlap_flat,
            .nested => .overlap_nested,
            .longest => .overlap_longest,
        });
        result.insert(switch (options.offset_unit) {
            .utf8_bytes => .offset_utf8,
            .unicode_codepoints => .offset_codepoints,
            .utf16_codeunits => .offset_utf16,
        });
        result.insert(switch (options.decoder.algorithm) {
            .auto => .decoder_auto,
            .exact => .decoder_exact,
            .beam => .decoder_beam,
        });
        if (options.decoder.best_effort) result.insert(.best_effort);
        switch (options.long_document.mode) {
            .reject => result.insert(.single_window),
            .window => {
                result.insert(.long_document);
                result.insert(switch (options.long_document.record_identity) {
                    .occurrence => .record_identity_occurrence,
                    .semantic => .record_identity_semantic,
                });
            },
        }
        if (options.include_confidence) result.insert(.confidence);
        if (options.include_spans) result.insert(.spans);
    }
    try check(control);
    return result;
}

/// Shared by qualification preflight and actual single-item execution. These
/// are existing service/model clamps, not policy-derived or inferred capacities.
pub fn singleProcessor(config: *const model.Config, item: *const wire.Item, supplied: processor.Options, max_queries: usize, control: ?Control) processor.Options {
    var out = item.options.preprocessing(supplied);
    out.control = control;
    out.max_text_words = @min(out.max_text_words, config.max_len);
    out.max_queries = @min(out.max_queries, max_queries);
    return out;
}

/// Shared exact word-window geometry. Identity hashing and observations remain
/// in the actual executor; quiet qualification planning emits no metrics.
pub fn longPlanning(config: *const model.Config, item: *const wire.Item, supplied: document.PlanOptions, control: ?Control) !document.PlanOptions {
    try check(control);
    if (item.options.long_document.mode != .window) return error.InvalidLongDocumentLimits;
    var out = supplied;
    out.mode = .windowed;
    out.word_splitter = item.options.word_splitter;
    out.max_window_body_words = @min(out.max_window_body_words, @min(item.options.long_document.window_words, config.max_len));
    out.overlap_words = item.options.long_document.overlap_words;
    if (out.overlap_words >= out.max_window_body_words) return error.InvalidExtractionOptions;
    out.max_windows = @min(out.max_windows, item.options.long_document.max_windows);
    out.other_record_identity = item.options.long_document.record_identity;
    out.control = control;
    return out;
}

pub fn windowProcessor(item: *const wire.Item, supplied: processor.Options, planning: document.PlanOptions, max_sequence_tokens: usize, control: ?Control) processor.Options {
    var out = item.options.preprocessing(supplied);
    out.control = control;
    out.max_text_words = @min(out.max_text_words, planning.max_window_body_words);
    out.max_sequence_tokens = @min(out.max_sequence_tokens, max_sequence_tokens);
    return out;
}

pub fn sourceWords(allocator: Allocator, text: []const u8, options: processor.Options) !usize {
    const ranges = try processor.sourceWordRanges(allocator, text, .{ .word_splitter = options.word_splitter, .max_text_bytes = options.max_text_bytes, .max_words = options.max_text_words, .control = options.control });
    defer allocator.free(ranges);
    try check(options.control);
    return ranges.len;
}

/// Scalar-only observation of one real serial prepared batch. Whole-document
/// source count comes from sourceWords() or the actual document plan; body word
/// and padded encoded dimensions come exclusively from the prepared batch.
pub fn lengths(request_items: usize, document_bytes: usize, document_words: usize, window_count: usize, prepared: *const processor.PreparedBatch) !policy.LengthContract {
    if (request_items == 0 or window_count == 0 or prepared.samples.len != 1 or prepared.sequence_length == 0) return error.UnsupportedGlinerBoundaryRuntime;
    const sample = prepared.samples[0];
    if (sample.input_ids.len == 0 or sample.input_ids.len > prepared.sequence_length) return error.UnsupportedGlinerBoundaryRuntime;
    return .{
        .request_items = policy.Range.exact(@intCast(request_items)),
        .document_bytes = policy.Range.exact(@intCast(document_bytes)),
        .document_words = policy.Range.exact(@intCast(document_words)),
        .window_count = policy.Range.exact(@intCast(window_count)),
        .window_words = policy.Range.exact(@intCast(sample.body_word_count)),
        .padded_sequence_tokens = policy.Range.exact(@intCast(prepared.sequence_length)),
    };
}

pub const Gate = struct {
    candidates: policy.Candidates,
    request_items: usize,
    control: ?Control,

    pub fn init(consumed: artifact.Identity, backend: policy.Backend, request: *const wire.Request, control: ?Control) !Gate {
        try check(control);
        // The current closed production policy performs no tokenizer work.
        if (!policy.hasPublishedProfiles()) return error.UnsupportedGlinerBoundaryRuntime;
        const features = try requiredFeatures(request, control);
        return .{ .candidates = try policy.start(consumed, backend, features), .request_items = request.items.len, .control = control };
    }

    pub fn observe(self: *Gate, document_bytes: usize, document_words: usize, window_count: usize, prepared: *const processor.PreparedBatch) !void {
        try check(self.control);
        try self.candidates.narrow(try lengths(self.request_items, document_bytes, document_words, window_count, prepared));
    }

    pub fn observeSingle(self: *Gate, allocator: Allocator, text: []const u8, prepared: *const processor.PreparedBatch, options: processor.Options) !void {
        const words = try sourceWords(allocator, text, options);
        try self.observe(text.len, words, 1, prepared);
    }
};

test "boundary qualification request feature union respects whole-item replacements" {
    var request = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"entities":["person"]},"options":{"include_spans":true},"inputs":[{"content":"Ada"},{"content":"Tokyo","schema":{"classifications":[{"name":"topic","labels":["a","b"],"max_labels":null,"ordered":false}]},"options":{"word_splitter":"char"}}]}
    , .{});
    defer request.deinit();
    const features = try requiredFeatures(&request, null);
    for ([_]policy.Feature{ .entities, .classification_single, .spans, .word_whitespace, .word_char, .single_window }) |feature| try std.testing.expect(features.contains(feature));
    try std.testing.expect(!features.contains(.joint_ie));
    try std.testing.expect(!features.contains(.long_document));
    try std.testing.expectEqual(@as(usize, 0), request.items[1].compiled.schema.entities.len);
    try std.testing.expect(!request.items[1].options.include_spans);
}

test "boundary qualification source words preserve Unicode and release every failed allocation" {
    const Exercise = struct {
        fn run(a: Allocator) !void {
            try std.testing.expectEqual(@as(usize, 2), try sourceWords(a, "Cafe 東京", .{}));
            try std.testing.expectEqual(@as(usize, 3), try sourceWords(a, "Cafe 東京", .{ .word_splitter = .char }));
            try std.testing.expectError(error.InvalidUtf8, sourceWords(a, "\xff", .{}));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
    try std.testing.expectError(error.BoundaryTextLimitExceeded, sourceWords(std.testing.allocator, "a b c", .{ .max_text_words = 2 }));
    const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
    var bounded = Bounded{ .backing = std.testing.allocator, .limit = 1 };
    try std.testing.expectError(error.OutOfMemory, sourceWords(bounded.allocator(), "one two", .{}));
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expect(bounded.denied);
}

test "boundary qualification request cancellation preserves schema and closed policy" {
    var request = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"entities":["person"]},"inputs":[{"content":"Ada"}]}
    , .{});
    defer request.deinit();
    const before = request.items[0].compiled.fingerprint;
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const control = Control{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, requiredFeatures(&request, control));
    try std.testing.expectError(error.Cancelled, sourceWords(std.testing.allocator, request.items[0].text, .{ .control = control }));
    const identity = artifact.Identity{ .backbone = .small, .precision = .fp32, .weight = artifact.Digest.of("test weight"), .sidecars = @splat(artifact.Digest.of("test sidecar")) };
    try std.testing.expectError(error.Cancelled, Gate.init(identity, .native, &request, control));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, Gate.init(identity, .native, &request, null));
    try std.testing.expectEqual(before, request.items[0].compiled.fingerprint);
}

test "boundary qualification geometry separates original source window and padded tokens" {
    // Borrowed scalar metadata only; no arena, tensor or model is constructed.
    const sample = processor.Sample{ .original_text = "Cafe 東京", .schema_fingerprint = @splat(0), .input_ids = &.{ 1, 2, 3 }, .words = &.{}, .groups = &.{}, .queries = &.{}, .classification_labels = &.{}, .enum_choices = &.{}, .prefix_word_count = 7, .body_word_count = 3, .terminal_period_added = true, .is_joint_ie = false };
    var prepared: processor.PreparedBatch = undefined;
    prepared.samples = &.{sample};
    prepared.sequence_length = 17;
    const words = try sourceWords(std.testing.allocator, sample.original_text, .{});
    const observed = try lengths(5, sample.original_text.len, words, 4, &prepared);
    try std.testing.expectEqual(@as(u64, 5), observed.request_items.min);
    try std.testing.expectEqual(@as(u64, 11), observed.document_bytes.min);
    try std.testing.expectEqual(@as(u64, 2), observed.document_words.min);
    try std.testing.expectEqual(@as(u64, 4), observed.window_count.min);
    try std.testing.expectEqual(@as(u64, 3), observed.window_words.min);
    try std.testing.expectEqual(@as(u64, 17), observed.padded_sequence_tokens.min);
    prepared.sequence_length = 2;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, lengths(5, 11, 2, 4, &prepared));
    prepared.sequence_length = 17;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, lengths(0, 11, 2, 4, &prepared));
}

test "boundary qualification canonical feature matrix covers every task and option family" {
    const Case = struct {
        name: []const u8,
        schema: []const u8,
        features: []const policy.Feature,
        options: []const u8 = "{}",
        option_features: []const policy.Feature = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .decoder_auto, .single_window },
    };
    const cases = [_]Case{
        .{
            .name = "attributes",
            .schema =
            \\{"entities":["person","company"],"entity_attributes":{"status":{"labels":["active","former"],"multi_label":true,"applies_to":["person"],"qualify_labels":true}}}
            ,
            .features = &.{ .entities, .entity_attributes },
        },
        .{
            .name = "entity description",
            .schema =
            \\{"entities":["person"],"entity_definitions":{"person":{"description":"A named human"}}}
            ,
            .features = &.{ .entities, .schema_descriptions },
        },
        .{
            .name = "entity validator",
            .schema =
            \\{"entities":["person"],"entity_definitions":{"person":{"validators":[{"pattern":"[A-Z]+","flags":0}]}}}
            ,
            .features = &.{ .entities, .regex_validation },
        },
        .{
            .name = "record mode None",
            .schema =
            \\{"structures":{"row":{"fields":{"name":"str"}}}}
            ,
            .features = &.{.legacy_structures},
        },
        .{
            .name = "record mode natural",
            .schema =
            \\{"structures":{"row":{"fields":{"name":"str"},"mode":"natural"}}}
            ,
            .features = &.{.records_natural},
        },
        .{
            .name = "record mode latent",
            .schema =
            \\{"structures":{"row":{"fields":{"name":"str"},"mode":"latent"}}}
            ,
            .features = &.{.records_latent},
        },
        .{
            .name = "record mode anchorless",
            .schema =
            \\{"structures":{"row":{"fields":{"name":"str"},"mode":"anchorless"}}}
            ,
            .features = &.{.records_anchorless},
        },
        .{
            .name = "enum choice alias",
            .schema =
            \\{"structures":{"row":{"fields":{"status":{"enum":["active","former"]}}}}}
            ,
            .features = &.{ .legacy_structures, .field_choices },
        },
        .{
            .name = "choice field",
            .schema =
            \\{"structures":{"row":{"mode":"latent","fields":{"status":{"choices":["active","former"]}}}}}
            ,
            .features = &.{ .records_latent, .field_choices },
        },
        .{
            .name = "list rule",
            .schema =
            \\{"structures":{"row":{"fields":{"tags":"list"}}}}
            ,
            .features = &.{ .legacy_structures, .field_rules },
        },
        .{
            .name = "scalar cardinality",
            .schema =
            \\{"structures":{"row":{"fields":{"name":{"cardinality":"required_one"}}}}}
            ,
            .features = &.{ .legacy_structures, .field_rules },
        },
        .{
            .name = "exclusive field",
            .schema =
            \\{"structures":{"row":{"fields":{"name":{"exclusive":true}}}}}
            ,
            .features = &.{ .legacy_structures, .field_rules },
        },
        .{
            .name = "occurrence policy",
            .schema =
            \\{"structures":{"row":{"mode":"natural","occurrence_policy":"first","fields":{"name":"str"}}}}
            ,
            .features = &.{ .records_natural, .record_occurrence_policy },
        },
        .{
            .name = "field description",
            .schema =
            \\{"structures":{"row":{"fields":{"name":{"description":"Name in the text"}}}}}
            ,
            .features = &.{ .legacy_structures, .schema_descriptions },
        },
        .{
            .name = "field validator",
            .schema =
            \\{"structures":{"row":{"fields":{"name":{"validators":[{"pattern":"[A-Z]+","flags":0}]}}}}}
            ,
            .features = &.{ .legacy_structures, .regex_validation },
        },
        .{
            .name = "single default",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"]}]}
            ,
            .features = &.{.classification_single},
        },
        .{
            .name = "multi default",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"mode":"multi"}]}
            ,
            .features = &.{.classification_multi},
        },
        .{
            .name = "ordinal",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["low","high"],"mode":"ordinal"}]}
            ,
            .features = &.{ .classification_ordinal, .classification_structured },
        },
        .{
            .name = "explicit nullable maximum",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"max_labels":null}]}
            ,
            .features = &.{ .classification_single, .classification_structured },
        },
        .{
            .name = "constraint activates structured selection",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"]}],"classification_constraints":[{"type":"Cardinality","task":"topic","minimum":1,"maximum":1}]}
            ,
            .features = &.{ .classification_single, .classification_structured, .classification_constraints },
        },
        .{
            .name = "classification prompt",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"prompt":"Choose a topic"}]}
            ,
            .features = &.{ .classification_single, .classification_context },
        },
        .{
            .name = "classification instruction",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"instruction":"Choose a topic"}]}
            ,
            .features = &.{ .classification_single, .classification_context },
        },
        .{
            .name = "classification hypothesis_template",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"hypothesis_template":"This text is about {}"}]}
            ,
            .features = &.{ .classification_single, .classification_context },
        },
        .{
            .name = "classification examples",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"examples":[["Example","a"]]}]}
            ,
            .features = &.{ .classification_single, .classification_context },
        },
        .{
            .name = "classification label description",
            .schema =
            \\{"classifications":[{"name":"topic","labels":["a","b"],"label_definitions":{"a":{"description":"Label A"}}}]}
            ,
            .features = &.{ .classification_single, .schema_descriptions },
        },
        .{
            .name = "standalone relation",
            .schema =
            \\{"relations":[{"type":"works_for"}]}
            ,
            .features = &.{.relations},
        },
        .{
            .name = "typed relation",
            .schema =
            \\{"relations":[{"type":"works_for","source":"person","target":"company"}]}
            ,
            .features = &.{ .relations, .relation_endpoints },
        },
        .{
            .name = "one typed endpoint",
            .schema =
            \\{"relations":[{"type":"works_for","source":"person"}]}
            ,
            .features = &.{ .relations, .relation_endpoints },
        },
        .{
            .name = "relation description",
            .schema =
            \\{"relations":[{"type":"works_for","description":"An employment relation"}]}
            ,
            .features = &.{ .relations, .schema_descriptions },
        },
        .{
            .name = "joint unconstrained",
            .schema =
            \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"]}}}}
            ,
            .features = &.{.joint_ie},
        },
        .{
            .name = "joint constraints",
            .schema =
            \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"]}},"constraints":[{"type":"AcyclicRelation","relation":"works_for"},{"type":"NoSelfLoops"}]}}
            ,
            .features = &.{ .joint_ie, .joint_constraints },
        },
        .{
            .name = "joint entity description",
            .schema =
            \\{"joint_ie":{"entities":{"person":{"description":"A named human"}}}}
            ,
            .features = &.{ .joint_ie, .schema_descriptions },
        },
        .{
            .name = "joint relation description",
            .schema =
            \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"],"description":"Employment"}}}}
            ,
            .features = &.{ .joint_ie, .schema_descriptions },
        },
        .{
            .name = "nested codepoint exact",
            .schema =
            \\{"entities":["person"]}
            ,
            .features = &.{.entities},
            .options =
            \\{"overlap":"nested","offset_unit":"unicode_codepoints","decoder":{"algorithm":"exact"},"include_confidence":true,"include_spans":true}
            ,
            .option_features = &.{ .word_whitespace, .overlap_nested, .offset_codepoints, .decoder_exact, .single_window, .confidence, .spans },
        },
        .{
            .name = "long semantic beam",
            .schema =
            \\{"entities":["person"]}
            ,
            .features = &.{.entities},
            .options =
            \\{"word_splitter":"char","overlap":"longest","offset_unit":"utf16_codeunits","decoder":{"algorithm":"beam","best_effort":true},"long_document":{"mode":"window","window_words":4,"overlap_words":1,"record_identity":"semantic"}}
            ,
            .option_features = &.{ .word_char, .overlap_longest, .offset_utf16, .decoder_beam, .best_effort, .long_document, .record_identity_semantic },
        },
        .{
            .name = "long occurrence auto",
            .schema =
            \\{"entities":["person"]}
            ,
            .features = &.{.entities},
            .options =
            \\{"long_document":{"mode":"window","window_words":4,"overlap_words":1}}
            ,
            .option_features = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .decoder_auto, .long_document, .record_identity_occurrence },
        },
    };
    var covered = policy.Features.initEmpty();
    for (cases) |case| {
        errdefer std.debug.print("qualification feature case: {s}\n", .{case.name});
        const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
        var bounded = Bounded{ .backing = std.testing.allocator, .limit = 2 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const a = bounded.allocator();
        var regex = @import("../pipelines/extraction_regex.zig").Context.init(a, .{
            .max_patterns = 4,
            .max_total_states = 128,
            .max_total_compile_steps = 16384,
        });
        defer regex.deinit();
        const bytes = try std.fmt.allocPrint(a, "{{\"schema_version\":2,\"model\":\"boundary\",\"schema\":{s},\"options\":{s},\"inputs\":[{{\"content\":\"Ada 東京\"}}]}}", .{ case.schema, case.options });
        defer a.free(bytes);
        var request = try wire.parseJson(a, bytes, .{ .compiler = regex.compilerOptions(.{}) });
        defer request.deinit();
        var expected = policy.Features.initEmpty();
        for (case.features) |feature| expected.insert(feature);
        for (case.option_features) |feature| expected.insert(feature);
        const before_fingerprint = request.items[0].compiled.fingerprint;
        const before_peak = bounded.peak;
        const actual = try requiredFeatures(&request, null);
        // Derivation is allocation-free and cannot mutate compiled semantics.
        try std.testing.expectEqual(before_peak, bounded.peak);
        try std.testing.expectEqual(before_fingerprint, request.items[0].compiled.fingerprint);
        inline for (@typeInfo(policy.Feature).@"enum".fields) |field| {
            const feature: policy.Feature = @enumFromInt(field.value);
            errdefer std.debug.print("qualification feature: {s}\n", .{field.name});
            try std.testing.expectEqual(expected.contains(feature), actual.contains(feature));
            if (actual.contains(feature)) covered.insert(feature);
        }
        try std.testing.expect(!bounded.denied);
    }
    // A new policy feature must acquire an actual canonical-wire test case.
    inline for (@typeInfo(policy.Feature).@"enum".fields) |field| {
        try std.testing.expect(covered.contains(@enumFromInt(field.value)));
    }
}
