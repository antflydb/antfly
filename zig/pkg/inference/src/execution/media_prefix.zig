// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned prepared prefix for full-recompute compiled decoding. Media
//! rows survive every text step; no loaded-model cache owns request values.
const std = @import("std");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Session = @import("../backends/session.zig").Session;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Prefix = struct {
    embeddings: Tensor,
    ple_ids: ?[]i64 = null,
    axis_positions: ?[]u32 = null,
    position_delta: i64 = 0,
    visibility: ?[]u8 = null,

    pub fn deinit(self: *Prefix, a: std.mem.Allocator) void {
        self.embeddings.deinit();
        if (self.ple_ids) |ids| a.free(ids);
        if (self.axis_positions) |positions| a.free(positions);
        if (self.visibility) |mask| a.free(mask);
        self.* = undefined;
    }

    pub fn run(self: *const Prefix, session: Session, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        const prefix_rows: usize = @intCast(self.embeddings.shape[1]);
        if (history.len < prefix_rows) return error.InvalidPreparedPrompt;
        var inputs: std.ArrayList(Tensor) = .empty;
        defer {
            for (inputs.items) |*input| input.deinit();
            inputs.deinit(a);
        }
        try inputs.ensureTotalCapacity(a, 5);
        inputs.appendAssumeCapacity(try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(history.len) }, history));
        inputs.appendAssumeCapacity(self.embeddings.borrowedView("input_embeddings"));
        if (self.ple_ids) |prefix| {
            if (prefix.len != prefix_rows) return error.InvalidPreparedPrompt;
            const ids = try Tensor.initInt64(a, "ple_input_ids", &.{ 1, @intCast(history.len) }, history);
            @memcpy(std.mem.bytesAsSlice(i64, @as([]align(@alignOf(i64)) u8, @alignCast(ids.data)))[0..prefix_rows], prefix);
            inputs.appendAssumeCapacity(ids);
        }
        if (self.axis_positions) |prefix| {
            if (prefix.len != prefix_rows * 3) return error.InvalidPreparedPrompt;
            const positions = try a.alloc(i64, history.len * 3);
            defer a.free(positions);
            for (0..3) |axis| {
                for (0..prefix_rows) |row| positions[axis * history.len + row] = prefix[axis * prefix_rows + row];
                for (prefix_rows..history.len) |row| positions[axis * history.len + row] = std.math.add(i64, @intCast(row), self.position_delta) catch return error.InvalidInputShape;
            }
            inputs.appendAssumeCapacity(try Tensor.initInt64(a, "position_ids", &.{ 3, @intCast(history.len) }, positions));
        }
        if (self.visibility) |prefix| {
            if (prefix.len != prefix_rows * prefix_rows) return error.InvalidPreparedPrompt;
            const mask = create: {
                const zeros = try a.alloc(u8, try std.math.mul(usize, history.len, history.len));
                errdefer a.free(zeros);
                @memset(zeros, 0);
                const shape = try a.dupe(i64, &.{ @intCast(history.len), @intCast(history.len) });
                break :create Tensor{ .data = zeros, .dtype = .u8, .shape = shape, .name = "attention_or_mask", .allocator = a, .owns_data = true, .owns_shape = true };
            };
            for (0..prefix_rows) |row| @memcpy(mask.data[row * history.len ..][0..prefix_rows], prefix[row * prefix_rows ..][0..prefix_rows]);
            inputs.appendAssumeCapacity(mask);
        }
        const outputs = try session.runWithControl(inputs.items, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        if (outputs.len != 1 or !std.mem.eql(u8, outputs[0].name, "logits") or outputs[0].dtype != .f32) return error.InvalidModelProgramOutput;
        return a.dupe(f32, outputs[0].asFloat32());
    }
};
