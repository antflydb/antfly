// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional borrowed observation of decisions already made by a training
//! step. Observers must copy any data they retain before returning. This
//! decision observation adds no tensor transfers; the opt-in retained-value
//! diagnostic callback may perform its own bounded readbacks. Neither replaces
//! sealed replay fingerprints, which bind backend-local floating-point values.
const pool = @import("gliner_boundary_train_decisions.zig");
const matching = @import("gliner_boundary_matching.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
pub const BoundaryInputs = objectives.Input;
pub const RetainedValues = struct {
    graph: *const ml.Graph,
    id_map: []const ml.NodeId,
    ids: []const ml.NodeId,
    values: []const ?ops.CT,
};

pub const Relations = struct {
    query_indices: []const i32,
    text_indices: [4][]const i32,
    head_prefix_start: []const i32,
    head_prefix_end: []const i32,
    tail_prefix_start: []const i32,
    tail_prefix_end: []const i32,
    mask: []const bool,
    labels: []const f32,
};
pub const Event = union(enum) {
    pool: *const pool.Pool,
    relations: Relations,
    boundary_loss: *const objectives.Result,
    record: struct { group: usize, sample: usize, schema_group: usize, target: *const matching.TargetMap, matches: *const matching.Matches, logits: matching.Logits, loss_weight: f32 },
};
pub const Observer = struct {
    context: *anyopaque,
    observe: *const fn (*anyopaque, Event) anyerror!void,
    /// Optional borrowed inputs already read for the bounded loss controller.
    /// Observing them must not introduce device readbacks or mutate training.
    boundary_inputs: ?*const fn (*anyopaque, BoundaryInputs) anyerror!void = null,
    /// Opt-in diagnostic access to existing retained values. Callers own any
    /// additional readback budget; ordinary decision observation adds none.
    retained_values: ?*const fn (*anyopaque, RetainedValues) anyerror!void = null,

    pub fn emit(self: ?Observer, event: Event) !void {
        if (self) |observer| try observer.observe(observer.context, event);
    }
    pub fn inputs(self: ?Observer, value: BoundaryInputs) !void {
        if (self) |observer| if (observer.boundary_inputs) |callback| try callback(observer.context, value);
    }
    pub fn retained(self: ?Observer, value: RetainedValues) !void {
        if (self) |observer| if (observer.retained_values) |callback| try callback(observer.context, value);
    }
};
