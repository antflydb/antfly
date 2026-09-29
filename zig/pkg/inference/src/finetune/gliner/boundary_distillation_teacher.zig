// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A frozen GLiNER2.5 checkpoint as a feature-distillation teacher: the
//! student's items are prepared with the teacher's own tokenizer and encoded
//! with the serving DeBERTa kernels on the CPU, and the routed final states
//! are handed to the trainer. Word splitting and schema markers do not depend
//! on the tokenizer, so the teacher's routes align row for row with the
//! student's; any difference is rejected rather than silently misaligned.
const std = @import("std");
const native = @import("../../ops/native_compute.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const engine = @import("../../architectures/gliner/boundary_engine.zig");
const source_mod = @import("boundary_training_source.zig");
const trainer_mod = @import("boundary_native_trainer.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const SourceTeacher = struct {
    source: *source_mod.Source,
    compute: native.NativeCompute,
    options: processor.Options,
    limits: engine.Limits,
    identity: [32]u8,

    /// Borrows the verified source for the teacher's lifetime. The processor
    /// options must match the student's, so both split words identically.
    pub fn init(self: *SourceTeacher, a: Allocator, source: *source_mod.Source, options: processor.Options, limits: engine.Limits) !void {
        if (source.config.encoder.family != .deberta or source.config.neck != .none) return error.UnsupportedBoundaryDistillationTeacher;
        self.* = .{ .source = source, .compute = native.NativeCompute.init(a, &source.store, null), .options = options, .limits = limits, .identity = undefined };
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.antenna.distillation-teacher.v1\x00");
        hash.update(std.mem.asBytes(&source.identity.weight));
        for (source.identity.sidecars) |sidecar| hash.update(std.mem.asBytes(&sidecar));
        hash.update(@tagName(options.word_splitter));
        self.identity = hash.finalResult();
    }

    pub fn deinit(self: *SourceTeacher) void {
        self.compute.deinit();
        self.* = undefined;
    }

    pub fn teacher(self: *SourceTeacher) trainer_mod.Teacher {
        return .{ .ptr = self, .identity = self.identity, .encode = encode };
    }

    const Owned = struct {
        allocator: Allocator,
        result: engine.Result,

        fn release(raw: ?*anyopaque) void {
            const self: *Owned = @ptrCast(@alignCast(raw.?));
            self.result.deinit();
            self.allocator.destroy(self);
        }
    };

    fn encode(raw: *anyopaque, a: Allocator, items: []const processor.Item, student: *const processor.PreparedBatch, control: ?Control) !trainer_mod.TeacherStates {
        const self: *SourceTeacher = @ptrCast(@alignCast(raw));
        var options = self.options;
        options.control = control;
        var prepared = try processor.prepare(a, self.source.tokenizer(), items, options);
        defer prepared.deinit();
        try requireAligned(student, &prepared);
        const cb = self.compute.computeBackend();
        const owned = try a.create(Owned);
        errdefer a.destroy(owned);
        owned.* = .{ .allocator = a, .result = try engine.encodeNative(&cb, a, &self.source.config, &prepared, .{ .limits = self.limits, .control = control }) };
        const result = &owned.result;
        return .{ .text = result.text_states, .queries = result.query_states, .classifications = result.classification_states, .parents = result.parent_states, .context = owned, .release = Owned.release };
    }
};

/// Same route widths and the same valid rows, in the same order.
pub fn requireAligned(student: *const processor.PreparedBatch, teacher: *const processor.PreparedBatch) !void {
    if (student.samples.len != teacher.samples.len or student.word_width != teacher.word_width or student.query_width != teacher.query_width or
        student.classification_width != teacher.classification_width or student.group_width != teacher.group_width)
        return error.BoundaryDistillationRouteMismatch;
    for ([_][2][]const bool{
        .{ student.text_word_mask, teacher.text_word_mask },
        .{ student.query_marker_mask, teacher.query_marker_mask },
        .{ student.cls_marker_mask, teacher.cls_marker_mask },
        .{ student.parent_marker_mask, teacher.parent_marker_mask },
    }) |pair| if (!std.mem.eql(bool, pair[0], pair[1])) return error.BoundaryDistillationRouteMismatch;
}
