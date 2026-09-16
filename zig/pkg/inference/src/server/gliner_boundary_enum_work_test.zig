// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Model-free scalar-score fixture for the production enum decoder. The three
//! distinct pool spans cover a rejected enum, an admitted enum, and body text.
//! Each row consumes six enumTick steps, separately from literal matching.
const std = @import("std");
const Allocator = std.mem.Allocator;
const wire = @import("../extractors/extraction_v2.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const regex = @import("../pipelines/extraction_regex.zig");
const model = @import("../models/gliner_boundary.zig");
const scoring = pipeline.scoring;
const ops = @import("../architectures/gliner_boundary_ops.zig");
const head = @import("../architectures/gliner_boundary_head.zig");
const tasks = @import("../architectures/gliner_boundary_tasks.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Fixture = struct {
    request: wire.Request,
    validators: regex.Context,
    explicit_calls: usize = 0,
    record_calls: usize = 0,
    last_sample: ?usize = null,

    pub fn init(a: Allocator) !Fixture {
        var validators = regex.Context.init(a, .{});
        errdefer validators.deinit();
        const request = try wire.parseJson(a,
            \\{"schema_version":2,"model":"enum-test","inputs":[{"id":"first","content":"Z"},{"id":"second","content":"Z"}],"schema":{"structures":{"event":{"mode":"anchorless","fields":{"kind":{"type":"str","cardinality":"required_one","choices":["bad","good"],"validators":[{"type":"regex","pattern":"^good$","flags":0}]}}}}}}
        , .{ .compiler = validators.compilerOptions(.{}) });
        return .{ .request = request, .validators = validators };
    }

    pub fn deinit(self: *Fixture) void {
        self.request.deinit();
        self.validators.deinit();
        self.* = undefined;
    }

    pub fn reset(self: *Fixture) void {
        self.explicit_calls = 0;
        self.record_calls = 0;
        self.last_sample = null;
    }

    pub fn run(self: *Fixture, a: Allocator, max_steps: usize, control: ?Control) ![]u8 {
        const config = model.Config{
            .version = model.config_version,
            .architecture_version = model.architecture_version,
            .max_len = 64,
            .backbone = .small,
            .head = .{},
            .encoder = std.mem.zeroes(model.EncoderConfig),
        };
        const schemas = [_]*const schema.CompiledSchema{ &self.request.items[0].compiled, &self.request.items[1].compiled };
        var samples: [2]processor.Sample = undefined;
        for (&samples, schemas, self.request.items) |*sample, compiled, item| sample.* = .{
            .original_text = item.text,
            .schema_fingerprint = compiled.fingerprint,
            .input_ids = &.{},
            .words = &.{
                .{ .text = "bad", .source = null, .input_start = 0, .input_end = 1 },
                .{ .text = "good", .source = null, .input_start = 1, .input_end = 2 },
                .{ .text = "z", .source = .{ .start = 0, .end = 1 }, .input_start = 2, .input_end = 3 },
            },
            .groups = &.{},
            .queries = &.{.{ .kind = .field, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "kind", .marker_index = 0 }},
            .classification_labels = &.{},
            .enum_choices = &.{
                .{ .structure_index = 0, .field_index = 0, .choice_index = 0, .start = 0, .end = 1, .score_start = 0 },
                .{ .structure_index = 0, .field_index = 0, .choice_index = 1, .start = 1, .end = 2, .score_start = 1 },
            },
            .prefix_word_count = 2,
            .body_word_count = 1,
            .terminal_period_added = false,
            .is_joint_ie = false,
        };
        var query_mask = [_]bool{ true, true };
        var prepared = processor.PreparedBatch{
            .arena = std.heap.ArenaAllocator.init(a),
            .samples = &samples,
            .sequence_length = 0,
            .word_width = 3,
            .query_width = 1,
            .classification_width = 0,
            .group_width = 0,
            .input_ids = &.{},
            .attention_mask = &.{},
            .text_word_indices = &.{},
            .text_word_mask = &.{},
            .query_marker_indices = &.{},
            .query_marker_mask = &query_mask,
            .query_group_index = &.{},
            .cls_marker_indices = &.{},
            .cls_marker_mask = &.{},
            .cls_group_index = &.{},
            .parent_marker_indices = &.{},
            .parent_marker_mask = &.{},
        };
        defer prepared.deinit();
        var indices = [_]ops.Span{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 }, .{ .start = 2, .end = 3 } } ** 2;
        var valid = [_]bool{true} ** 6;
        const pool = ops.SharedPool{ .allocator = a, .batch = 2, .capacity = 3, .indices = &indices, .valid = &valid, .proposal_logits = &.{}, .compat_logits = &.{} };
        const scores = scoring.CandidateScoreView{ .batch = 2, .text_length = 3, .queries = 1, .pool = &pool, .pair_logits = &.{ 4, 4, 4, 4, 4, 4 }, .null_logits = null, .count_log_rates = null };
        var output = wire.ResponseWriter.init(a, 4096, self.request.items.len);
        defer output.deinit();
        try output.begin(self.request.model);
        var result = try pipeline.runScored(a, &config, &prepared, &schemas, scores, .{
            .context = self,
            .check_fn = check,
            .classify_fn = classify,
            .explicit_fn = explicit,
            .relations_fn = relations,
            .record_fn = record,
        }, .{
            .enum_limits = .{ .max_steps = max_steps },
            .control = control,
            .regex_context = &self.validators,
            .validate_value_fn = regex.Context.validateValue,
        });
        defer result.deinit();
        for (self.request.items, result.samples) |item, sample| try output.append(item, sample);
        return output.finish(0);
    }

    fn check(_: *anyopaque) !void {}
    fn classify(_: *anyopaque, _: Allocator, _: tasks.Limits, _: ?Control) !scoring.ClassificationScores {
        return error.UnexpectedScorerCall;
    }
    fn relations(_: *anyopaque, _: Allocator, _: scoring.RelationRequest, _: tasks.Limits, _: ?Control) !scoring.RelationScores {
        return error.UnexpectedScorerCall;
    }
    fn explicit(raw: *anyopaque, a: Allocator, request: scoring.ExplicitRequest, _: head.Limits, _: ?Control) !scoring.ExplicitScores {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.explicit_calls += 1;
        self.last_sample = request.sample_index;
        try std.testing.expectEqual(@as(usize, 2), request.indices.len);
        var storage = try scoring.Storage.init(a);
        errdefer storage.deinit();
        return .{ .storage = storage, .logits = try storage.alloc().dupe(f32, &.{ 10, 5 }), .valid = try storage.alloc().dupe(bool, &.{ true, true }) };
    }
    fn record(raw: *anyopaque, a: Allocator, request: scoring.RecordRequest, _: tasks.Limits, _: ?Control) !scoring.RecordScores {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.record_calls += 1;
        try std.testing.expectEqual(@as(usize, 1), request.fields.len);
        try std.testing.expectEqualSlices(usize, &.{ 1, 2 }, request.fields[0].pool_indices);
        var storage = try scoring.Storage.init(a);
        errdefer storage.deinit();
        const alloc = storage.alloc();
        const fields = try alloc.alloc(tasks.RecordFieldResult, 1);
        fields[0] = .{
            .candidate_spans = try alloc.dupe(ops.Span, &.{ .{ .start = 1, .end = 2 }, .{ .start = 2, .end = 3 } }),
            .candidate_logits = try alloc.dupe(f32, &.{ 4, 4 }),
            .assign_logits = try alloc.dupe(f32, &.{ -4, 3, 4 }),
        };
        return .{ .storage = storage, .object_logits = try alloc.dupe(f32, &.{5}), .instance_seeds = try alloc.dupe(?tasks.RecordSeed, &.{null}), .fields = fields };
    }
};
