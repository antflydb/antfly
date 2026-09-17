// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional borrowed observation of decisions already made by a training
//! step. Observers must copy any data they retain before returning. This
//! interface adds no tensor transfers and never replaces the sealed replay
//! fingerprints, which also bind exact backend-local floating-point values.
const pool = @import("gliner_boundary_train_decisions.zig");
const matching = @import("gliner_boundary_matching.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");

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
    record: struct { group: usize, sample: usize, schema_group: usize, target: *const matching.TargetMap, matches: *const matching.Matches },
};
pub const Observer = struct {
    context: *anyopaque,
    observe: *const fn (*anyopaque, Event) anyerror!void,

    pub fn emit(self: ?Observer, event: Event) !void {
        if (self) |observer| try observer.observe(observer.context, event);
    }
};
