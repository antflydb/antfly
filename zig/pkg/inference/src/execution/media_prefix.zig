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
    /// Zero preserves exact-shape execution; opt-in buckets bound padding.
    bucket_rows: usize = 0,
    retained: ?@import("../backends/session.zig").RunPermit = null,
    ple_ids: ?[]i64 = null,
    axis_positions: ?[]u32 = null,
    position_delta: i64 = 0,
    visibility: ?[]u8 = null,

    pub fn deinit(self: *Prefix, a: std.mem.Allocator) void {
        self.embeddings.deinit();
        if (self.ple_ids) |ids| a.free(ids);
        if (self.axis_positions) |positions| a.free(positions);
        if (self.visibility) |mask| a.free(mask);
        if (self.retained) |*permit| permit.deinit();
        self.* = undefined;
    }

    pub fn run(self: *const Prefix, session: Session, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (self.bucket_rows > 256) return error.InvalidInputShape;
        if (history.len == 0) return error.InvalidPreparedPrompt;
        const rows = if (self.bucket_rows == 0) history.len else bucket: {
            const cfg = @import("../architectures/session_factory.zig").getGptConfig(session) orelse return error.InvalidPreparedPrompt;
            const rounded = try std.math.mul(usize, try std.math.divCeil(usize, history.len, self.bucket_rows), self.bucket_rows);
            break :bucket @min(rounded, cfg.max_position_embeddings);
        };
        if (rows < history.len) return error.InvalidInputShape;
        // Staging exists before Session.run admits its execution. Keep its
        // real host charge until all argument copies have been released.
        var elements = try std.math.mul(usize, rows, 2 + @as(usize, if (self.ple_ids != null) 1 else 0));
        if (self.axis_positions != null) elements = try std.math.add(usize, elements, try std.math.mul(usize, rows, 6));
        var staging_bytes = try std.math.mul(usize, elements, @sizeOf(i64));
        if (self.visibility != null) staging_bytes = try std.math.add(usize, staging_bytes, try std.math.mul(usize, rows, rows));
        var staging = try session.withAdmissionControl(control).admitRetainedMedia(staging_bytes, 0);
        defer staging.deinit();
        const padded = if (rows != history.len) try a.alloc(i64, rows) else null;
        defer if (padded) |values| a.free(values);
        if (padded) |values| {
            @memcpy(values[0..history.len], history);
            @memset(values[history.len..], 0);
        }
        const padded_ids = padded orelse history;
        const prefix_rows: usize = @intCast(self.embeddings.shape[1]);
        if (history.len < prefix_rows) return error.InvalidPreparedPrompt;
        var inputs: std.ArrayList(Tensor) = .empty;
        defer {
            for (inputs.items) |*input| input.deinit();
            inputs.deinit(a);
        }
        try inputs.ensureTotalCapacity(a, 6);
        inputs.appendAssumeCapacity(try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(rows) }, padded_ids));
        if (self.bucket_rows != 0) inputs.appendAssumeCapacity(try Tensor.initInt64(a, "logits_row", &.{1}, &.{@intCast(history.len - 1)}));
        inputs.appendAssumeCapacity(self.embeddings.borrowedView("input_embeddings"));
        if (self.ple_ids) |prefix| {
            if (prefix.len != prefix_rows) return error.InvalidPreparedPrompt;
            const ids = try Tensor.initInt64(a, "ple_input_ids", &.{ 1, @intCast(rows) }, padded_ids);
            @memcpy(std.mem.bytesAsSlice(i64, @as([]align(@alignOf(i64)) u8, @alignCast(ids.data)))[0..prefix_rows], prefix);
            inputs.appendAssumeCapacity(ids);
        }
        if (self.axis_positions) |prefix| {
            if (prefix.len != prefix_rows * 3) return error.InvalidPreparedPrompt;
            const positions = try a.alloc(i64, rows * 3);
            defer a.free(positions);
            for (0..3) |axis| {
                for (0..prefix_rows) |row| positions[axis * rows + row] = prefix[axis * prefix_rows + row];
                for (prefix_rows..rows) |row| positions[axis * rows + row] = std.math.add(i64, @intCast(row), self.position_delta) catch return error.InvalidInputShape;
            }
            inputs.appendAssumeCapacity(try Tensor.initInt64(a, "position_ids", &.{ 3, @intCast(rows) }, positions));
        }
        if (self.visibility) |prefix| {
            if (prefix.len != prefix_rows * prefix_rows) return error.InvalidPreparedPrompt;
            const mask = create: {
                const zeros = try a.alloc(u8, try std.math.mul(usize, rows, rows));
                errdefer a.free(zeros);
                @memset(zeros, 0);
                const shape = try a.dupe(i64, &.{ @intCast(rows), @intCast(rows) });
                break :create Tensor{ .data = zeros, .dtype = .u8, .shape = shape, .name = "attention_or_mask", .allocator = a, .owns_data = true, .owns_shape = true };
            };
            for (0..prefix_rows) |row| @memcpy(mask.data[row * rows ..][0..prefix_rows], prefix[row * prefix_rows ..][0..prefix_rows]);
            inputs.appendAssumeCapacity(mask);
        }
        const outputs = try session.runWithControl(inputs.items, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        if (outputs.len != 1 or !std.mem.eql(u8, outputs[0].name, "logits") or outputs[0].dtype != .f32) return error.InvalidModelProgramOutput;
        return a.dupe(f32, outputs[0].asFloat32());
    }
};
