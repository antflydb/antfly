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

//! Request-side inputs to the closed runtime policy. This module derives
//! features from compiled schemas and geometry from real preparation/planning.
//! It neither loads release evidence nor changes inference semantics.
const std = @import("std");
const policy = @import("../models/gliner_boundary_qualification.zig");
const artifact = @import("../models/gliner_boundary_bundle.zig");
const model = @import("../models/gliner_boundary.zig");
const wire = @import("extraction_v2.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const document = @import("../pipelines/gliner_boundary_long_document.zig");
const OffsetUnit = @import("../pipelines/gliner_boundary_decode.zig").OffsetUnit;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

pub fn requiredFeatures(request: *const wire.Request, control: ?Control) !policy.Features {
    return requiredFeaturesResolved(request, .{}, control);
}

fn decoderFeature(algorithm: anytype) policy.Feature {
    return switch (algorithm) {
        .auto => .decoder_auto,
        .exact => .decoder_exact,
        .beam => .decoder_beam,
    };
}

/// The qualified path binds the actual service defaults as well as explicit
/// per-item selectors. Diagnostic execution may supply different defaults;
/// those must never borrow another decoder profile's qualification row.
pub fn requiredFeaturesResolved(request: *const wire.Request, common: pipeline.Options, control: ?Control) !policy.Features {
    try check(control);
    if (request.items.len == 0) return error.InvalidExtractionRequest;
    var result = policy.Features.empty;
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
            .allow => .overlap_allow,
            .nested => .overlap_nested,
            .longest => .overlap_longest,
        });
        result.insert(switch (options.offset_unit) {
            .utf8_bytes => .offset_utf8,
            .unicode_codepoints => .offset_codepoints,
            .utf16_codeunits => .offset_utf16,
        });
        const resolved = options.native(common);
        if (schema.joint_ie != null) {
            const joint = if (options.long_document.mode == .window) document.globalJointOptions(resolved.joint_solver) else resolved.joint_solver;
            switch (joint.profile) {
                .fastino_v1 => {
                    if (joint.algorithm != .beam) return error.UnsupportedGlinerBoundaryRuntime;
                    result.insert(.joint_fastino_v1);
                },
                .native => result.insert(decoderFeature(joint.algorithm)),
            }
        }
        // Keep classification requirements when a typed request contains both
        // families. For other tasks retain the existing native option flags.
        if (schema.joint_ie == null or schema.classifications.len != 0)
            result.insert(decoderFeature(resolved.classification_solver.algorithm));
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
        return initResolved(consumed, backend, request, .{}, control);
    }

    pub fn initResolved(consumed: artifact.Identity, backend: policy.Backend, request: *const wire.Request, common: pipeline.Options, control: ?Control) !Gate {
        try check(control);
        // The current closed production policy performs no tokenizer work.
        if (!policy.hasPublishedProfiles()) return error.UnsupportedGlinerBoundaryRuntime;
        const features = try requiredFeaturesResolved(request, common, control);
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

test "boundary qualification distinguishes single-window source and explicit or global native decoders" {
    var request = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"joint_ie":{"entities":{"person":{}}}},"inputs":[
        \\{"content":"Ada"},
        \\{"content":"Ada","options":{"decoder":{"beam_width":16}}},
        \\{"content":"Ada","options":{"decoder":{"algorithm":"auto"}}},
        \\{"content":"Ada","options":{"decoder":{"algorithm":"exact"}}},
        \\{"content":"Ada","options":{"decoder":{"algorithm":"beam"}}},
        \\{"content":"Ada","schema":{"classifications":[{"name":"topic","labels":["a","b"]}]}},
        \\{"content":"Ada","options":{"long_document":{"mode":"window","window_words":4,"overlap_words":1}}},
        \\{"content":"Ada","options":{"long_document":{"mode":"window","window_words":4,"overlap_words":1},"decoder":{"algorithm":"beam"}}}
        \\]}
    , .{});
    defer request.deinit();
    const expected = [_]policy.Feature{ .joint_fastino_v1, .joint_fastino_v1, .decoder_auto, .decoder_exact, .decoder_beam, .decoder_auto, .decoder_auto, .decoder_beam };
    try std.testing.expectEqual(expected.len, request.items.len);
    for (expected, 0..) |wanted, index| {
        var single = request;
        single.items = request.items[index .. index + 1];
        const actual = try requiredFeatures(&single, null);
        for ([_]policy.Feature{ .joint_fastino_v1, .decoder_auto, .decoder_exact, .decoder_beam }) |feature|
            try std.testing.expectEqual(feature == wanted, actual.contains(feature));
        try std.testing.expectEqual(index != 5, actual.contains(.joint_ie));
        try std.testing.expectEqual(index == 5, actual.contains(.classification_single));
        try std.testing.expectEqual(index >= 6, actual.contains(.long_document));
    }
    var union_expected = policy.Features.empty;
    for ([_]policy.Feature{ .joint_ie, .classification_single, .joint_fastino_v1, .decoder_auto, .decoder_exact, .decoder_beam, .word_whitespace, .overlap_flat, .offset_utf8, .single_window, .long_document, .record_identity_occurrence }) |feature|
        union_expected.insert(feature);
    const actual_union = try requiredFeatures(&request, null);
    inline for (@typeInfo(policy.Feature).@"enum".field_names, @typeInfo(policy.Feature).@"enum".field_values) |_, field_value| {
        const feature: policy.Feature = @fromBackingInt(field_value);
        try std.testing.expectEqual(union_expected.contains(feature), actual_union.contains(feature));
    }
}

test "boundary qualification resolved decoder features follow service defaults and mixed tasks" {
    var request = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"joint_ie":{"entities":{"person":{}}}},"inputs":[{"content":"Ada"}]}
    , .{});
    defer request.deinit();
    var classification = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"classifications":[{"name":"topic","labels":["a","b"]}]},"inputs":[{"content":"Ada"}]}
    , .{});
    defer classification.deinit();
    var common = pipeline.Options{};
    common.classification_solver.algorithm = .beam;
    common.joint_solver = .{ .profile = .native, .algorithm = .exact };
    var features = try requiredFeaturesResolved(&request, common, null);
    try std.testing.expect(features.contains(.decoder_exact));
    try std.testing.expect(!features.contains(.joint_fastino_v1) and !features.contains(.decoder_auto) and !features.contains(.decoder_beam));
    features = try requiredFeaturesResolved(&classification, common, null);
    try std.testing.expect(features.contains(.decoder_beam) and !features.contains(.decoder_auto));

    // Borrow both schemas without modifying either owner. The public parser
    // currently forbids this combination, but an internal mixed task must
    // require both actual decoder contracts if that restriction changes.
    var mixed_items = [_]wire.Item{request.items[0]};
    mixed_items[0].compiled.schema.classifications = classification.items[0].compiled.schema.classifications;
    var mixed = request;
    mixed.items = &mixed_items;
    common.joint_solver = .{ .profile = .fastino_v1, .algorithm = .beam };
    features = try requiredFeaturesResolved(&mixed, common, null);
    try std.testing.expect(features.contains(.joint_fastino_v1) and features.contains(.decoder_beam));
    try std.testing.expect(!features.contains(.decoder_auto) and !features.contains(.decoder_exact));

    mixed_items[0].options.decoder.algorithm = .exact;
    features = try requiredFeaturesResolved(&mixed, common, null);
    try std.testing.expect(features.contains(.decoder_exact));
    try std.testing.expect(!features.contains(.joint_fastino_v1) and !features.contains(.decoder_auto) and !features.contains(.decoder_beam));
    mixed_items[0].options.decoder.algorithm = null;
    mixed_items[0].options.long_document.mode = .window;
    features = try requiredFeaturesResolved(&mixed, common, null);
    try std.testing.expect(features.contains(.decoder_auto) and features.contains(.decoder_beam));
    try std.testing.expect(!features.contains(.joint_fastino_v1));
    common.joint_solver = .{ .profile = .native, .algorithm = .exact };
    features = try requiredFeaturesResolved(&mixed, common, null);
    try std.testing.expect(features.contains(.decoder_exact) and features.contains(.decoder_beam));
    try std.testing.expect(!features.contains(.decoder_auto) and !features.contains(.joint_fastino_v1));

    common.joint_solver = .{ .profile = .fastino_v1, .algorithm = .exact };
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, requiredFeaturesResolved(&request, common, null));
}

test "boundary qualification source words preserve Unicode and release every failed allocation" {
    const Exercise = struct {
        fn run(a: Allocator) !void {
            try std.testing.expectEqual(@as(usize, 2), try sourceWords(a, "Cafe 東京", .{}));
            try std.testing.expectEqual(@as(usize, 3), try sourceWords(a, "Cafe 東京", .{ .word_splitter = .char }));
            try std.testing.expectError(error.InvalidUtf8, sourceWords(a, "\xff", .{}));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
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
            .option_features = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .joint_fastino_v1, .single_window },
        },
        .{
            .name = "joint constraints",
            .schema =
            \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"]}},"constraints":[{"type":"AcyclicRelation","relation":"works_for"},{"type":"NoSelfLoops"}]}}
            ,
            .features = &.{ .joint_ie, .joint_constraints },
            .option_features = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .joint_fastino_v1, .single_window },
        },
        .{
            .name = "joint entity description",
            .schema =
            \\{"joint_ie":{"entities":{"person":{"description":"A named human"}}}}
            ,
            .features = &.{ .joint_ie, .schema_descriptions },
            .option_features = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .joint_fastino_v1, .single_window },
        },
        .{
            .name = "joint relation description",
            .schema =
            \\{"joint_ie":{"entities":{"person":{},"company":{}},"relations":{"works_for":{"head":["person"],"tail":["company"],"description":"Employment"}}}}
            ,
            .features = &.{ .joint_ie, .schema_descriptions },
            .option_features = &.{ .word_whitespace, .overlap_flat, .offset_utf8, .joint_fastino_v1, .single_window },
        },
        .{
            .name = "allow overlap explicit",
            .schema =
            \\{"entities":["person"]}
            ,
            .features = &.{.entities},
            .options =
            \\{"overlap":"allow"}
            ,
            .option_features = &.{ .word_whitespace, .overlap_allow, .offset_utf8, .decoder_auto, .single_window },
        },
        .{
            .name = "allow overlap flat_ner alias",
            .schema =
            \\{"entities":["person"]}
            ,
            .features = &.{.entities},
            .options =
            \\{"flat_ner":false}
            ,
            .option_features = &.{ .word_whitespace, .overlap_allow, .offset_utf8, .decoder_auto, .single_window },
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
    var covered = policy.Features.empty;
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
        var expected = policy.Features.empty;
        for (case.features) |feature| expected.insert(feature);
        for (case.option_features) |feature| expected.insert(feature);
        const before_fingerprint = request.items[0].compiled.fingerprint;
        const before_peak = bounded.peak;
        const actual = try requiredFeatures(&request, null);
        // Derivation is allocation-free and cannot mutate compiled semantics.
        try std.testing.expectEqual(before_peak, bounded.peak);
        try std.testing.expectEqual(before_fingerprint, request.items[0].compiled.fingerprint);
        inline for (@typeInfo(policy.Feature).@"enum".field_names, @typeInfo(policy.Feature).@"enum".field_values) |reflected_name, field_value| {
            const feature: policy.Feature = @fromBackingInt(field_value);
            errdefer std.debug.print("qualification feature: {s}\n", .{reflected_name});
            try std.testing.expectEqual(expected.contains(feature), actual.contains(feature));
            if (actual.contains(feature)) covered.insert(feature);
        }
        try std.testing.expect(!bounded.denied);
    }
    // A new policy feature must acquire an actual canonical-wire test case.
    inline for (@typeInfo(policy.Feature).@"enum".field_names, @typeInfo(policy.Feature).@"enum".field_values) |_, field_value| {
        try std.testing.expect(covered.contains(@fromBackingInt(field_value)));
    }
}

const FamilyGeometryRow = struct {
    id: []const u8,
    text: []const u8,
    native_schema_json: []const u8,
    encoded: struct { input_ids: []const i64 },
};

fn expectFamilyFeatures(id: []const u8, actual: policy.Features, decide: bool) !void {
    var expected = policy.Features.initMany(&.{ .classification_single, .word_whitespace, .overlap_flat, .decoder_auto, .single_window, .confidence });
    if (decide) {
        expected.insert(.classification_context);
        expected.insert(.schema_descriptions);
        expected.insert(.offset_utf8);
    } else {
        expected.remove(.classification_single);
        expected.insert(.offset_codepoints);
        expected.insert(.spans);
        if (std.mem.eql(u8, id, "english_full_task")) {
            expected.insert(.entities);
            expected.insert(.schema_descriptions);
            expected.insert(.classification_single);
            expected.insert(.legacy_structures);
            expected.insert(.relations);
        } else if (std.mem.endsWith(u8, id, "_entities")) {
            expected.insert(.entities);
            expected.insert(.schema_descriptions);
        } else {
            expected.insert(.classification_single);
            expected.insert(.classification_structured);
            if (std.mem.eql(u8, id, "described_labels")) expected.insert(.schema_descriptions);
            if (std.mem.eql(u8, id, "instruction_prompt")) expected.insert(.classification_context);
        }
    }
    inline for (@typeInfo(policy.Feature).@"enum".field_values) |field_value| {
        const feature: policy.Feature = @fromBackingInt(field_value);
        errdefer std.debug.print("family geometry feature {s}: {s}\n", .{ id, @tagName(feature) });
        try std.testing.expectEqual(expected.contains(feature), actual.contains(feature));
    }
}

fn measureFamilyRow(a: Allocator, tok: anytype, identity: artifact.Identity, row: FamilyGeometryRow, decide: bool) !policy.LengthContract {
    var schema = try std.json.parseFromSlice(std.json.Value, a, row.native_schema_json, .{ .duplicate_field_behavior = .@"error" });
    defer schema.deinit();
    const body = if (decide)
        try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "boundary",
            .schema = schema.value,
            .options = .{ .include_confidence = true },
            .inputs = &.{.{ .content = row.text }},
        }, .{})
    else
        try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "boundary",
            .schema = schema.value,
            .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
            .inputs = &.{.{ .content = row.text }},
        }, .{});
    defer a.free(body);
    var request = try wire.parseJson(a, body, .{});
    defer request.deinit();
    const features = try requiredFeatures(&request, null);
    try expectFamilyFeatures(row.id, features, decide);
    const item = &request.items[0];
    var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, item.options.preprocessing(.{}));
    defer prepared.deinit();
    try std.testing.expectEqualSlices(i64, row.encoded.input_ids, prepared.samples[0].input_ids);
    const words = try sourceWords(a, item.text, item.options.preprocessing(.{}));
    const observed = try lengths(1, item.text.len, words, 1, &prepared);
    try policy.require(identity, .native, features, observed);
    try policy.require(identity, .metal, features, observed);
    if (!decide) {
        const utf8_body = try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "boundary",
            .schema = schema.value,
            .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "utf8_bytes" },
            .inputs = &.{.{ .content = row.text }},
        }, .{});
        defer a.free(utf8_body);
        var utf8_request = try wire.parseJson(a, utf8_body, .{});
        defer utf8_request.deinit();
        const utf8_features = try requiredFeatures(&utf8_request, null);
        inline for (@typeInfo(policy.Feature).@"enum".field_values) |field_value| {
            const feature: policy.Feature = @fromBackingInt(field_value);
            const expected = if (feature == .offset_codepoints)
                false
            else if (feature == .offset_utf8)
                true
            else
                features.contains(feature);
            try std.testing.expectEqual(expected, utf8_features.contains(feature));
        }
        try policy.require(identity, .native, utf8_features, observed);
        try policy.require(identity, .metal, utf8_features, observed);
    }
    std.debug.print("GLiNER2.5 family geometry {s}: bytes={d} document_words={d} window_words={d} padded_sequence_tokens={d}\n", .{
        row.id,
        observed.document_bytes.min,
        observed.document_words.min,
        observed.window_words.min,
        observed.padded_sequence_tokens.min,
    });
    return observed;
}

const FamilyClassificationGroup = enum { bare, described, context, context_described, mixed };
const FamilyEntityGroup = enum { bare, described };

fn expectExactFamilyLengths(actual: policy.LengthContract, expected: policy.LengthContract) !void {
    try std.testing.expectEqual(expected.request_items, actual.request_items);
    try std.testing.expectEqual(expected.document_bytes, actual.document_bytes);
    try std.testing.expectEqual(expected.document_words, actual.document_words);
    try std.testing.expectEqual(expected.window_count, actual.window_count);
    try std.testing.expectEqual(expected.window_words, actual.window_words);
    try std.testing.expectEqual(expected.padded_sequence_tokens, actual.padded_sequence_tokens);
}

fn measureFamilyEntityRow(
    a: Allocator,
    tok: anytype,
    identity: artifact.Identity,
    row: FamilyGeometryRow,
    group: FamilyEntityGroup,
    offset: OffsetUnit,
) !policy.LengthContract {
    var schema = try std.json.parseFromSlice(std.json.Value, a, row.native_schema_json, .{ .duplicate_field_behavior = .@"error" });
    defer schema.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "boundary",
        .schema = schema.value,
        .options = .{
            .include_confidence = true,
            .include_spans = true,
            .offset_unit = if (offset == .unicode_codepoints) "unicode_codepoints" else "utf8_bytes",
        },
        .inputs = &.{.{ .content = row.text }},
    }, .{});
    defer a.free(body);
    var request = try wire.parseJson(a, body, .{});
    defer request.deinit();
    const features = try requiredFeatures(&request, null);
    var expected = policy.Features.initMany(&.{
        .entities,
        .word_whitespace,
        .overlap_flat,
        .decoder_auto,
        .single_window,
        .confidence,
        .spans,
    });
    expected.insert(if (offset == .unicode_codepoints) .offset_codepoints else .offset_utf8);
    if (group == .described) expected.insert(.schema_descriptions);
    inline for (@typeInfo(policy.Feature).@"enum".field_values) |field_value| {
        const feature: policy.Feature = @fromBackingInt(field_value);
        errdefer std.debug.print("family entity geometry feature {s}: {s}\n", .{ row.id, @tagName(feature) });
        try std.testing.expectEqual(expected.contains(feature), features.contains(feature));
    }
    const item = &request.items[0];
    var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, item.options.preprocessing(.{}));
    defer prepared.deinit();
    try std.testing.expectEqualSlices(i64, row.encoded.input_ids, prepared.samples[0].input_ids);
    const words = try sourceWords(a, item.text, item.options.preprocessing(.{}));
    const observed = try lengths(1, item.text.len, words, 1, &prepared);
    const source_word_floor = std.mem.endsWith(u8, row.id, "source_word_floor");
    const expected_lengths: policy.LengthContract = if (std.mem.endsWith(u8, row.id, "long"))
        .{
            .request_items = policy.Range.exact(1),
            .document_bytes = policy.Range.exact(663),
            .document_words = policy.Range.exact(114),
            .window_count = policy.Range.exact(1),
            .window_words = policy.Range.exact(114),
            .padded_sequence_tokens = policy.Range.exact(if (group == .bare) 181 else 202),
        }
    else
        .{
            .request_items = policy.Range.exact(1),
            .document_bytes = policy.Range.exact(if (source_word_floor) 5 else if (std.mem.indexOf(u8, row.id, "clean") != null) 2 else 1),
            .document_words = policy.Range.exact(if (source_word_floor) 1 else if (std.mem.indexOf(u8, row.id, "clean") != null) 2 else 1),
            .window_count = policy.Range.exact(1),
            .window_words = policy.Range.exact(2),
            .padded_sequence_tokens = policy.Range.exact(if (source_word_floor)
                if (group == .bare) 17 else 22
            else if (group == .bare) 18 else 24),
        };
    try expectExactFamilyLengths(observed, expected_lengths);
    if (observed.document_bytes.min == 1) {
        // Several broader learned-feature rows cover individual dimensions
        // of this shape, but no single reviewed entity row covers their
        // combination. The policy deliberately reports its conservative
        // cross-row fallback dimension rather than pretending the byte axis
        // alone explains the refusal.
        try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, policy.require(identity, .native, features, observed));
        try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, policy.require(identity, .metal, features, observed));
    } else {
        try policy.require(identity, .native, features, observed);
        try policy.require(identity, .metal, features, observed);
    }
    std.debug.print("GLiNER2.5 family entity geometry {s}/{s}/{s}: bytes={d} document_words={d} window_words={d} padded_sequence_tokens={d}\n", .{
        row.id,
        @tagName(group),
        @tagName(offset),
        observed.document_bytes.min,
        observed.document_words.min,
        observed.window_words.min,
        observed.padded_sequence_tokens.min,
    });
    return observed;
}

fn entityGroup(id: []const u8) !FamilyEntityGroup {
    if (std.mem.startsWith(u8, id, "bare_")) return .bare;
    if (std.mem.startsWith(u8, id, "described_")) return .described;
    return error.InvalidFamilyReference;
}

fn measureFamilyClassificationRow(
    a: Allocator,
    tok: anytype,
    identity: artifact.Identity,
    row: FamilyGeometryRow,
    group: FamilyClassificationGroup,
    offset: OffsetUnit,
) !policy.LengthContract {
    var schema = try std.json.parseFromSlice(std.json.Value, a, row.native_schema_json, .{ .duplicate_field_behavior = .@"error" });
    defer schema.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "boundary",
        .schema = schema.value,
        .options = .{
            .include_confidence = true,
            .include_spans = true,
            .offset_unit = if (offset == .unicode_codepoints) "unicode_codepoints" else "utf8_bytes",
        },
        .inputs = &.{.{ .content = row.text }},
    }, .{});
    defer a.free(body);
    var request = try wire.parseJson(a, body, .{});
    defer request.deinit();
    const features = try requiredFeatures(&request, null);
    var expected = policy.Features.initMany(&.{
        .classification_single,
        .classification_structured,
        .word_whitespace,
        .overlap_flat,
        .decoder_auto,
        .single_window,
        .confidence,
        .spans,
    });
    expected.insert(if (offset == .unicode_codepoints) .offset_codepoints else .offset_utf8);
    switch (group) {
        .bare => {},
        .described => expected.insert(.schema_descriptions),
        .context => expected.insert(.classification_context),
        .context_described => {
            expected.insert(.classification_context);
            expected.insert(.schema_descriptions);
        },
        .mixed => {
            for ([_]policy.Feature{ .entities, .relations, .legacy_structures, .classification_context, .schema_descriptions }) |feature| expected.insert(feature);
        },
    }
    inline for (@typeInfo(policy.Feature).@"enum".field_values) |field_value| {
        const feature: policy.Feature = @fromBackingInt(field_value);
        errdefer std.debug.print("family classification geometry feature {s}: {s}\n", .{ row.id, @tagName(feature) });
        try std.testing.expectEqual(expected.contains(feature), features.contains(feature));
    }
    const item = &request.items[0];
    var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = item.text, .schema = &item.compiled }}, item.options.preprocessing(.{}));
    defer prepared.deinit();
    try std.testing.expectEqualSlices(i64, row.encoded.input_ids, prepared.samples[0].input_ids);
    const words = try sourceWords(a, item.text, item.options.preprocessing(.{}));
    const observed = try lengths(1, item.text.len, words, 1, &prepared);
    try policy.require(identity, .native, features, observed);
    try policy.require(identity, .metal, features, observed);
    std.debug.print("GLiNER2.5 family classification geometry {s}/{s}/{s}: bytes={d} document_words={d} window_words={d} padded_sequence_tokens={d}\n", .{
        row.id,
        @tagName(group),
        @tagName(offset),
        observed.document_bytes.min,
        observed.document_words.min,
        observed.window_words.min,
        observed.padded_sequence_tokens.min,
    });
    return observed;
}

fn classificationGroup(id: []const u8) !FamilyClassificationGroup {
    if (std.mem.eql(u8, id, "one_character_two_label") or std.mem.eql(u8, id, "bare_long")) return .bare;
    if (std.mem.startsWith(u8, id, "described_")) return .described;
    if (std.mem.startsWith(u8, id, "prompt_described_")) return .context_described;
    if (std.mem.startsWith(u8, id, "prompt_")) return .context;
    return error.InvalidFamilyReference;
}

fn familyIdentity(checkpoint: pipeline.FamilyCheckpoint) !artifact.Identity {
    var identity = artifact.Identity{
        .backbone = .multi,
        .precision = .fp32,
        .weight = .{ .size_bytes = checkpoint.model.size_bytes, .sha256 = undefined },
        .sidecars = undefined,
    };
    if (checkpoint.model.sha256.len != identity.weight.sha256.len) return error.InvalidFamilyReference;
    @memcpy(&identity.weight.sha256, checkpoint.model.sha256);
    inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" }, 0..) |name, index| {
        const pin = @field(checkpoint.sidecars, name);
        if (pin.sha256.len != identity.sidecars[index].sha256.len) return error.InvalidFamilyReference;
        identity.sidecars[index].size_bytes = pin.size_bytes;
        @memcpy(&identity.sidecars[index].sha256, pin.sha256);
    }
    return identity;
}

// Reproducible policy measurement over the exact committed Python captures.
// Loads tokenizer/config sidecars only; model weights are never opened.
test "GLiNER2.5 multilingual family qualification measures exact captured wire geometry" {
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const c_file = @import("../util/c_file.zig");
    const platform = @import("antfly_platform");
    const a = std.testing.allocator;
    inline for (pipeline.multilingual_family_checkpoints, 0..) |checkpoint, checkpoint_index| {
        const environment = checkpoint.environment_prefix ++ "_MODEL_DIR";
        const directory = platform.env.getenv(environment) orelse return error.SkipZigTest;
        const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
        defer a.free(tokenizer_path);
        const tokenizer_bytes = try c_file.readFileMax(a, tokenizer_path, checkpoint.sidecars.@"tokenizer.json".size_bytes);
        defer a.free(tokenizer_bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(tokenizer_bytes, &digest, .{});
        const tokenizer_hash = std.fmt.bytesToHex(digest, .lower);
        try std.testing.expectEqualStrings(checkpoint.sidecars.@"tokenizer.json".sha256, &tokenizer_hash);
        const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
        defer tokenizer.tokenizer().deinitTokenizer();
        const capture_bytes = try fixtures.fixtureBytes(a, checkpoint.capture_fixture);
        defer a.free(capture_bytes);
        const Capture = struct { requests: []const FamilyGeometryRow };
        var capture = try std.json.parseFromSlice(Capture, a, capture_bytes, .{ .ignore_unknown_fields = true });
        defer capture.deinit();
        try std.testing.expectEqual(@as(usize, 11), capture.value.requests.len);
        const identity = try familyIdentity(checkpoint);
        for (capture.value.requests) |row| _ = try measureFamilyRow(a, tokenizer, identity, row, false);

        inline for (.{
            pipeline.multilingual_endpoint_checkpoints[checkpoint_index],
            pipeline.multilingual_classification_floor_checkpoints[checkpoint_index],
            pipeline.multilingual_general_classification_checkpoints[checkpoint_index],
        }, 0..) |extra_checkpoint, capture_index| {
            const extra_bytes = try fixtures.fixtureBytes(a, extra_checkpoint.capture_fixture);
            defer a.free(extra_bytes);
            var extra_capture = try std.json.parseFromSlice(Capture, a, extra_bytes, .{ .ignore_unknown_fields = true });
            defer extra_capture.deinit();
            try std.testing.expectEqual(extra_checkpoint.request_count, extra_capture.value.requests.len);
            for (extra_capture.value.requests) |row| {
                const group: FamilyClassificationGroup = if (capture_index == 0) .mixed else try classificationGroup(row.id);
                _ = try measureFamilyClassificationRow(a, tokenizer, identity, row, group, .unicode_codepoints);
                _ = try measureFamilyClassificationRow(a, tokenizer, identity, row, group, .utf8_bytes);
            }
        }

        inline for (.{
            pipeline.multilingual_general_entity_checkpoints[checkpoint_index],
            pipeline.multilingual_clean_entity_short_checkpoints[checkpoint_index],
            pipeline.multilingual_source_word_floor_checkpoints[checkpoint_index],
        }) |entity_checkpoint| {
            const entity_bytes = try fixtures.fixtureBytes(a, entity_checkpoint.capture_fixture);
            defer a.free(entity_bytes);
            var entity_capture = try std.json.parseFromSlice(Capture, a, entity_bytes, .{ .ignore_unknown_fields = true });
            defer entity_capture.deinit();
            try std.testing.expectEqual(entity_checkpoint.request_count, entity_capture.value.requests.len);
            for (entity_capture.value.requests) |row| {
                const group = try entityGroup(row.id);
                _ = try measureFamilyEntityRow(a, tokenizer, identity, row, group, .unicode_codepoints);
                _ = try measureFamilyEntityRow(a, tokenizer, identity, row, group, .utf8_bytes);
            }
        }
    }

    const directory = platform.env.getenv("ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFileMax(a, tokenizer_path, pipeline.multilingual_family_checkpoints[1].sidecars.@"tokenizer.json".size_bytes);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const decide_bytes = try fixtures.fixtureBytes(a, "family/multi_decide_decide_capture.json");
    defer a.free(decide_bytes);
    const DecideCapture = struct { requests: []const FamilyGeometryRow };
    var decide_capture = try std.json.parseFromSlice(DecideCapture, a, decide_bytes, .{ .ignore_unknown_fields = true });
    defer decide_capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), decide_capture.value.requests.len);
    const identity = try familyIdentity(pipeline.multilingual_family_checkpoints[1]);
    for (decide_capture.value.requests) |row| _ = try measureFamilyRow(a, tokenizer, identity, row, true);
}

// Evidence-gathering, not a synthetic contract check: this measures the exact
// geometry the pinned base checkpoint's own tokenizer produces for the
// shortest and longest known qualification requests, using the real
// preparation path. The production row in ../models/gliner_boundary_qualification.zig
// is reviewed against these printed numbers; a future narrowing of that row
// below an observed value here is a release regression, not just a test edit.
test "gliner boundary qualification measures pinned base checkpoint production geometry" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const c_file = @import("../util/c_file.zig");
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const tokenizer_bytes = try c_file.readFile(a, path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();

    const Case = struct { name: []const u8, text: []const u8, body: []const u8 };
    const cases = [_]Case{
        .{
            .name = "shortest canonical fixture (enum_field/constrained_classification length)",
            .text = "Delete the temporary file.",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["object"]},"inputs":[{"content":"Delete the temporary file."}]}
            ,
        },
        .{
            .name = "repro request (entities + relations + confidence + spans)",
            .text = "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection.",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["component","subsystem","test"],"relations":[{"type":"depends_on"},{"type":"tested_by"}]},"options":{"include_confidence":true,"include_spans":true},"inputs":[{"content":"The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection."}]}
            ,
        },
        .{
            .name = "examples/dogfood real production schema (11 entities, 6 relations) on the repro text",
            .text = "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection.",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]},"options":{"include_confidence":true,"include_spans":true},"inputs":[{"content":"The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection."}]}
            ,
        },
        .{
            // examples/dogfood's smallest real section: zig/SCHEMA.md's
            // "Related Docs" list, which docsaf reduces to the link targets
            // with no whitespace between them -- one 20-byte "word". The
            // ingest embeds and full-text indexes it, so extraction must
            // admit it too rather than fail the whole drain.
            .name = "examples/dogfood real production schema on the smallest real corpus section (SCHEMA.md Related Docs, 20 bytes/1 word)",
            .text = "TODO.mdSERVERLESS.md",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]},"options":{"include_confidence":true,"include_spans":true,"long_document":{"mode":"window"}},"inputs":[{"content":"TODO.mdSERVERLESS.md"}]}
            ,
        },
        .{
            .name = "examples/dogfood real production schema on a one-character document",
            .text = "a",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]},"options":{"include_confidence":true,"include_spans":true,"long_document":{"mode":"window"}},"inputs":[{"content":"a"}]}
            ,
        },
        .{
            .name = "examples/dogfood real production schema on a realistic corpus paragraph (ENRICHMENTS.md, 734 bytes/107 words)",
            .text = "Both lanes are handed the same document group's classified work and, when both have real work for the quantum, are scheduled with `Io.concurrent` so their provider round trips overlap; the calling task runs the dense lane inline while awaiting the concurrently spawned asset lane. If the `Io` backend does not support concurrency (for example a deterministic single-flow VOPR/simulation harness), both lanes still run, just sequentially, with identical outcomes -- concurrency is a scheduling optimization, not a correctness requirement. In-flight work is bounded to exactly one preparation quantum per stream.",
            .body =
            \\{"schema_version":2,"model":"boundary","schema":{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]},"options":{"include_confidence":true,"include_spans":true},"inputs":[{"content":"Both lanes are handed the same document group's classified work and, when both have real work for the quantum, are scheduled with `Io.concurrent` so their provider round trips overlap; the calling task runs the dense lane inline while awaiting the concurrently spawned asset lane. If the `Io` backend does not support concurrency (for example a deterministic single-flow VOPR/simulation harness), both lanes still run, just sequentially, with identical outcomes -- concurrency is a scheduling optimization, not a correctness requirement. In-flight work is bounded to exactly one preparation quantum per stream."}]}
            ,
        },
    };
    var min = policy.LengthContract{
        .request_items = policy.Range.exact(1),
        .document_bytes = .{ .min = std.math.maxInt(u64), .max = std.math.maxInt(u64) },
        .document_words = .{ .min = std.math.maxInt(u64), .max = std.math.maxInt(u64) },
        .window_count = policy.Range.exact(1),
        .window_words = .{ .min = std.math.maxInt(u64), .max = std.math.maxInt(u64) },
        .padded_sequence_tokens = .{ .min = std.math.maxInt(u64), .max = std.math.maxInt(u64) },
    };
    var max = policy.LengthContract{
        .request_items = policy.Range.exact(1),
        .document_bytes = .{ .min = 0, .max = 0 },
        .document_words = .{ .min = 0, .max = 0 },
        .window_count = policy.Range.exact(1),
        .window_words = .{ .min = 0, .max = 0 },
        .padded_sequence_tokens = .{ .min = 0, .max = 0 },
    };
    for (cases) |case| {
        var request = try wire.parseJson(a, case.body, .{});
        defer request.deinit();
        const words = try sourceWords(a, case.text, .{});
        var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = case.text, .schema = &request.items[0].compiled }}, .{});
        defer prepared.deinit();
        const observed = try lengths(1, case.text.len, words, 1, &prepared);
        std.debug.print(
            "gliner boundary base geometry [{s}]: document_bytes={} document_words={} window_words={} padded_sequence_tokens={}\n",
            .{ case.name, observed.document_bytes.min, observed.document_words.min, observed.window_words.min, observed.padded_sequence_tokens.min },
        );
        min.document_bytes.min = @min(min.document_bytes.min, observed.document_bytes.min);
        min.document_words.min = @min(min.document_words.min, observed.document_words.min);
        min.window_words.min = @min(min.window_words.min, observed.window_words.min);
        min.padded_sequence_tokens.min = @min(min.padded_sequence_tokens.min, observed.padded_sequence_tokens.min);
        max.document_bytes.max = @max(max.document_bytes.max, observed.document_bytes.max);
        max.document_words.max = @max(max.document_words.max, observed.document_words.max);
        max.window_words.max = @max(max.window_words.max, observed.window_words.max);
        max.padded_sequence_tokens.max = @max(max.padded_sequence_tokens.max, observed.padded_sequence_tokens.max);
    }

    // The ten canonical single-window task fixtures for the pinned base
    // checkpoint (entities, relations, classification, records, JointIE,
    // Unicode) are exercised end to end and cross-checked against the Python
    // reference by the native and Metal parity tests above. Sweep their exact
    // documents through the same real tokenizer to bound the schema-inflated
    // encoded sequence length actually produced for this feature set.
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const fixture_bytes = try fixtures.fixtureBytes(a, "pipeline_cases_base.json");
    defer a.free(fixture_bytes);
    const parsed = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, fixture_bytes, .{});
    defer parsed.deinit();
    const Input = struct { content: []const u8 };
    const Envelope = struct { schema_version: u32 = 2, model: []const u8 = "boundary", schema: std.json.Value, inputs: []const Input };
    for (parsed.value.cases) |case| {
        const body = try std.json.Stringify.valueAlloc(a, Envelope{ .schema = case.schema, .inputs = &.{.{ .content = case.text }} }, .{});
        defer a.free(body);
        var request = try wire.parseJson(a, body, .{});
        defer request.deinit();
        const words = try sourceWords(a, case.text, .{});
        var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = case.text, .schema = &request.items[0].compiled }}, .{});
        defer prepared.deinit();
        const observed = try lengths(1, case.text.len, words, 1, &prepared);
        std.debug.print(
            "gliner boundary base geometry [fixture {s}]: document_bytes={} document_words={} window_words={} padded_sequence_tokens={}\n",
            .{ case.id, observed.document_bytes.min, observed.document_words.min, observed.window_words.min, observed.padded_sequence_tokens.min },
        );
        min.document_bytes.min = @min(min.document_bytes.min, observed.document_bytes.min);
        min.document_words.min = @min(min.document_words.min, observed.document_words.min);
        min.window_words.min = @min(min.window_words.min, observed.window_words.min);
        min.padded_sequence_tokens.min = @min(min.padded_sequence_tokens.min, observed.padded_sequence_tokens.min);
        max.document_bytes.max = @max(max.document_bytes.max, observed.document_bytes.max);
        max.document_words.max = @max(max.document_words.max, observed.document_words.max);
        max.window_words.max = @max(max.window_words.max, observed.window_words.max);
        max.padded_sequence_tokens.max = @max(max.padded_sequence_tokens.max, observed.padded_sequence_tokens.max);
    }
    std.debug.print(
        "gliner boundary base geometry SUMMARY: document_bytes=[{},{}] document_words=[{},{}] window_words=[{},{}] padded_sequence_tokens=[{},{}]\n",
        .{
            min.document_bytes.min,         max.document_bytes.max,
            min.document_words.min,         max.document_words.max,
            min.window_words.min,           max.window_words.max,
            min.padded_sequence_tokens.min, max.padded_sequence_tokens.max,
        },
    );
}

/// Slices out the section beginning at the first occurrence of `heading`
/// (a full Markdown heading line, e.g. "### Some Title") and running up to
/// (but excluding) the next line that starts a new Markdown heading, or to
/// the end of the file. Mirrors the section boundaries examples/dogfood's
/// docsaf.MarkdownProcessor produces (a new section at every heading line),
/// so the measured document is the same shape a real ingest would extract.
fn extractHeadingSection(full: []const u8, heading: []const u8) ![]const u8 {
    const start = std.mem.indexOf(u8, full, heading) orelse return error.MissingFixtureSection;
    var end = full.len;
    var cursor = start + heading.len;
    while (cursor + 1 < full.len) : (cursor += 1) {
        if (full[cursor] == '\n' and full[cursor + 1] == '#') {
            end = cursor;
            break;
        }
    }
    return full[start..end];
}

const LongDocumentCase = struct { name: []const u8, path: ?[]const u8 = null, heading: []const u8 = "", text: ?[]const u8 = null };

// Paths are relative to the inference-test binary's working directory
// (zig/pkg/inference, per zig/TESTING.md's build steps). examples/dogfood
// requests long_document.mode=window unconditionally for every section (see
// index_config.go's knowledgeGraphIndexJSON), so the qualified row must also
// cover the small end of the real corpus, not just the sections that
// actually require more than one window.
const long_document_geometry_cases = [_]LongDocumentCase{
    // The two smallest documents the row must admit: examples/dogfood's
    // smallest real section (zig/SCHEMA.md's "Related Docs" list, which
    // docsaf reduces to the two link targets with no whitespace between
    // them) and a one-character document. Both are embedded and full-text
    // indexed by the ingest, so extraction must admit them too.
    .{ .name = "SCHEMA.md \"Related Docs\" (smallest real corpus section, 20 bytes), windowed", .text = "TODO.mdSERVERLESS.md" },
    .{ .name = "one-character document, windowed", .text = "a" },
    .{ .name = "shortest canonical fixture, windowed", .text = "Delete the temporary file." },
    .{ .name = "ENRICHMENTS.md realistic short paragraph, windowed (734 bytes/107 words)", .text = "Both lanes are handed the same document group's classified work and, when both have real work for the quantum, are scheduled with `Io.concurrent` so their provider round trips overlap; the calling task runs the dense lane inline while awaiting the concurrently spawned asset lane. If the `Io` backend does not support concurrency (for example a deterministic single-flow VOPR/simulation harness), both lanes still run, just sequentially, with identical outcomes -- concurrency is a scheduling optimization, not a correctness requirement. In-flight work is bounded to exactly one preparation quantum per stream." },
    .{ .name = "LSM.md \"Read And Scan Work\" (~p95 real section size, 6.8KB)", .path = "../antfly/src/storage/lsm/LSM.md", .heading = "### Read And Scan Work" },
    .{ .name = "VOPR.md \"Completion-Claim Audit\" (37KB real section)", .path = "../../VOPR.md", .heading = "### Completion-Claim Audit" },
    .{ .name = "PDF.md \"Review findings and required fixes\" (max real section across zig/*.md and work-log/**/*.md, 99KB)", .path = "../../PDF.md", .heading = "## Review findings and required fixes" },
    // Synthetic documents built by concatenating zig/PDF.md's two largest
    // real sections (verbatim, per a docsaf-accurate Go-side sweep of the
    // whole ingest corpus that also confirmed no single real section
    // currently exceeds ~94KB), fixtured under testdata/gliner25/. A
    // production incident (see GLINER25.md's long-document section) traced 9
    // dogfood extraction failures to batched multi-item requests, not
    // document size -- but qualifying past the corpus's current real
    // single-section maximum, with real measured margin instead of
    // extrapolation, is still the right defensive posture for corpus growth.
    .{ .name = "synthetic 130KB (PDF.md sections concatenated)", .path = "testdata/gliner25/long_document_probe/combo130.txt" },
    .{ .name = "synthetic 182KB/28275-word (PDF.md sections concatenated)", .path = "testdata/gliner25/long_document_probe/combo182.txt" },
};

/// Measures exact window geometry for one (window_words, overlap_words) pair
/// across `long_document_geometry_cases`, printing a per-case line and a
/// SUMMARY line, and returns the overall observed LengthContract. Pure
/// tokenizer/planner measurement: no model weights are loaded.
fn measureLongDocumentGeometry(
    a: std.mem.Allocator,
    tok: anytype,
    config: *const model.Config,
    parsed_schema: std.json.Value,
    window_words: u64,
    overlap_words: u64,
    label: []const u8,
) !policy.LengthContract {
    const c_file = @import("../util/c_file.zig");
    var overall = policy.LengthContract{
        .request_items = policy.Range.exact(1),
        .document_bytes = .{ .min = std.math.maxInt(u64), .max = 0 },
        .document_words = .{ .min = std.math.maxInt(u64), .max = 0 },
        .window_count = .{ .min = std.math.maxInt(u64), .max = 0 },
        .window_words = .{ .min = std.math.maxInt(u64), .max = 0 },
        .padded_sequence_tokens = .{ .min = std.math.maxInt(u64), .max = 0 },
    };
    for (long_document_geometry_cases) |case| {
        var owned_full: ?[]const u8 = null;
        defer if (owned_full) |full| a.free(full);
        const section_text = if (case.text) |literal| literal else blk: {
            const path = case.path.?;
            const full = c_file.readFile(a, path) catch |err| {
                std.debug.print("gliner boundary long-document geometry: skipping unavailable fixture {s} ({s}): {s}\n", .{ case.name, path, @errorName(err) });
                continue;
            };
            owned_full = full;
            break :blk if (case.heading.len == 0) full else try extractHeadingSection(full, case.heading);
        };
        const body = try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "boundary",
            .schema = parsed_schema,
            .options = .{ .include_confidence = true, .include_spans = true, .long_document = .{ .mode = "window", .window_words = window_words, .overlap_words = overlap_words } },
            .inputs = &.{.{ .content = section_text }},
        }, .{});
        defer a.free(body);
        var request = try wire.parseJson(a, body, .{});
        defer request.deinit();
        const item = &request.items[0];

        const planning = try longPlanning(config, item, .{}, null);
        var doc = try document.plan(a, item.text, item.compiled.fingerprint, planning);
        defer doc.deinit();
        // Whole-document word counting is unrelated to the per-window body
        // cap (processor.Options.max_text_words defaults to 4096): raise it
        // here so counting the full document does not itself hit that limit.
        const words = try sourceWords(a, item.text, .{ .max_text_words = 1 << 20, .max_text_bytes = 4 * 1024 * 1024 });

        var min_window_words: u64 = std.math.maxInt(u64);
        var max_window_words: u64 = 0;
        var min_padded: u64 = std.math.maxInt(u64);
        var max_padded: u64 = 0;
        for (doc.windows) |window| {
            const window_text = try doc.windowText(window.index);
            const window_options = windowProcessor(item, .{}, planning, 16384, null);
            var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = window_text, .schema = &item.compiled }}, window_options);
            defer prepared.deinit();
            const observed = try lengths(1, item.text.len, words, doc.windows.len, &prepared);
            min_window_words = @min(min_window_words, observed.window_words.min);
            max_window_words = @max(max_window_words, observed.window_words.max);
            min_padded = @min(min_padded, observed.padded_sequence_tokens.min);
            max_padded = @max(max_padded, observed.padded_sequence_tokens.max);
        }
        std.debug.print(
            "gliner boundary base LONG-DOCUMENT geometry [{s}, window_words={}] [{s}]: document_bytes={} document_words={} window_count={} window_words=[{},{}] padded_sequence_tokens=[{},{}]\n",
            .{ label, window_words, case.name, item.text.len, words, doc.windows.len, min_window_words, max_window_words, min_padded, max_padded },
        );
        overall.document_bytes.min = @min(overall.document_bytes.min, item.text.len);
        overall.document_bytes.max = @max(overall.document_bytes.max, item.text.len);
        overall.document_words.min = @min(overall.document_words.min, words);
        overall.document_words.max = @max(overall.document_words.max, words);
        overall.window_count.min = @min(overall.window_count.min, doc.windows.len);
        overall.window_count.max = @max(overall.window_count.max, doc.windows.len);
        overall.window_words.min = @min(overall.window_words.min, min_window_words);
        overall.window_words.max = @max(overall.window_words.max, max_window_words);
        overall.padded_sequence_tokens.min = @min(overall.padded_sequence_tokens.min, min_padded);
        overall.padded_sequence_tokens.max = @max(overall.padded_sequence_tokens.max, max_padded);
    }
    std.debug.print(
        "gliner boundary base LONG-DOCUMENT geometry SUMMARY [{s}, window_words={}]: document_bytes=[{},{}] document_words=[{},{}] window_count=[{},{}] window_words=[{},{}] padded_sequence_tokens=[{},{}]\n",
        .{
            label,                              window_words,
            overall.document_bytes.min,         overall.document_bytes.max,
            overall.document_words.min,         overall.document_words.max,
            overall.window_count.min,           overall.window_count.max,
            overall.window_words.min,           overall.window_words.max,
            overall.padded_sequence_tokens.min, overall.padded_sequence_tokens.max,
        },
    );
    return overall;
}

// Evidence for long-document windowing qualification: real repository
// sections at representative sizes (see GLINER25.md's long-document
// qualification section for the corpus-wide percentile/max survey that
// picked these), against the real examples/dogfood production schema (11
// entities, 6 relations). Sweeps the wire's window_words options actually
// under consideration for the default (see GLINER25.md's throughput section
// for why 1024 was chosen over the original 4096 default: attention cost is
// quadratic in window length, and a live-server throughput sweep on Metal
// showed 1024-word windows completing real sections several times faster
// than 4096-word windows, at the cost of more windows per long document).
test "gliner boundary qualification measures pinned base checkpoint long-document production geometry" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const c_file = @import("../util/c_file.zig");
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();

    const config_path = try std.fs.path.join(a, &.{ directory, "config.json" });
    defer a.free(config_path);
    const config_bytes = try c_file.readFile(a, config_path);
    defer a.free(config_bytes);
    const encoder_path = try std.fs.path.join(a, &.{ directory, "encoder_config", "config.json" });
    defer a.free(encoder_path);
    const encoder_bytes = try c_file.readFile(a, encoder_path);
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);

    const schema_source =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
    ;
    var parsed_schema = try std.json.parseFromSlice(std.json.Value, a, schema_source, .{});
    defer parsed_schema.deinit();

    // Proportional overlap: window_words/32, matching the original 4096/128
    // ratio (~3.125%).
    _ = try measureLongDocumentGeometry(a, tok, &config, parsed_schema.value, 1024, 32, "candidate default");
    _ = try measureLongDocumentGeometry(a, tok, &config, parsed_schema.value, 4096, 128, "prior default, for comparison");
}
