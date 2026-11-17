// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit request cache, outside loaded-model caches and execution lanes.
//! The reservation pins its source model until all provider buffers retire.
const std = @import("std");
const enabled = @import("build_options").enable_pjrt;
const Resident = if (enabled) @import("../graph/pjrt_decoder_state.zig").State else opaque {};
const Session = @import("../backends/session.zig").Session;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Config = @import("../models/gpt.zig").Config;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Decoder = struct {
    allocator: std.mem.Allocator,
    source: Session,
    config: Config,
    capacity: usize,
    limit: usize,
    bank_bytes: usize,
    resident: ?*Resident = null,
    position: usize = 0,
    axis_positions: bool = false,
    position_delta: i64 = 0,
    failed: bool = false,
    control: ?Control = null,
    reservation: @import("../backends/session.zig").RunPermit,

    pub fn init(a: std.mem.Allocator, source: Session, limit: usize, control: ?Control) !Decoder {
        const cfg = (source.model_contract.decoder_config orelse return error.UnsupportedResidentDecode).*;
        if (!enabled or source.backend() != .pjrt or cfg.position_encoding != .rope) return error.UnsupportedResidentDecode;
        const capacity = try capacityFor(limit, cfg.max_position_embeddings);
        var elements: usize = 0;
        for (0..cfg.num_hidden_layers) |layer| {
            const layer_elements = if (cfg.layerUsesQwen35LinearAttention(layer)) linear: {
                const channels = try std.math.add(usize, try std.math.mul(usize, 2, try std.math.mul(usize, cfg.qwen35_linear_num_key_heads, cfg.qwen35_linear_key_head_dim)), try std.math.mul(usize, cfg.qwen35_linear_num_value_heads, cfg.qwen35_linear_value_head_dim));
                if (cfg.qwen35_linear_conv_kernel_dim == 0) return error.InvalidModelConfig;
                const conv = try std.math.mul(usize, channels, cfg.qwen35_linear_conv_kernel_dim - 1);
                const recurrent = try std.math.mul(usize, cfg.qwen35_linear_num_value_heads, try std.math.mul(usize, cfg.qwen35_linear_key_head_dim, cfg.qwen35_linear_value_head_dim));
                break :linear try std.math.add(usize, conv, recurrent);
            } else try std.math.mul(usize, 2 * capacity, try std.math.mul(usize, cfg.effectiveKVHeadsForLayer(layer), cfg.effectiveHeadDimForLayer(layer)));
            elements = try std.math.add(usize, elements, layer_elements);
        }
        const bytes = try std.math.mul(usize, elements, @sizeOf(f32));
        // Two non-donated generations coexist through successful publication;
        // host zero staging is bounded conservatively by one complete bank.
        const reservation = try source.withAdmissionControl(control).admitRetainedMedia(try std.math.add(usize, bytes, @sizeOf(f32)), try std.math.add(usize, try std.math.mul(usize, bytes, 2), 2 * @sizeOf(f32)));
        return .{ .allocator = a, .source = source, .config = cfg, .capacity = capacity, .limit = limit, .bank_bytes = bytes, .reservation = reservation, .control = control };
    }
    pub fn deinit(self: *Decoder) void {
        const active = self.control orelse Control{ .request_owned = false, .native_call_policy = .in_process };
        var protection = active.enterUninterruptible(self.source.interruption()) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        defer protection.deinit();
        if (comptime enabled) if (self.resident) |state| {
            state.deinit();
            self.allocator.destroy(state);
        };
        self.reservation.deinit();
        self.* = undefined;
    }
    pub fn geometry(self: *const Decoder, inputs: []const Tensor) !@import("../backends/session.zig").RunGeometry {
        for (inputs) |input| if (std.mem.eql(u8, input.name, "input_ids")) {
            if (input.dtype != .i64 or input.shape.len != 2 or input.shape[0] != 1 or input.shape[1] <= 0) return error.InvalidInputShape;
            const rows: usize = @intCast(input.shape[1]);
            var valid_rows = rows;
            for (inputs) |control_input| if (std.mem.eql(u8, control_input.name, "logits_row")) {
                if (control_input.dtype != .i64 or control_input.shape.len != 1 or control_input.shape[0] != 1 or control_input.data.len != @sizeOf(i64) or !control_input.isAlignedFor(i64)) return error.InvalidInputShape;
                const row = control_input.asInt64()[0];
                if (row < 0 or row >= @as(i64, @intCast(rows))) return error.InvalidInputShape;
                valid_rows = @intCast(row + 1);
            };
            if (self.position > self.limit or valid_rows > self.limit - self.position) return error.InvalidResidentDecodePosition;
            return @import("decoder_frontend.zig").geometry(self.config, rows, self.capacity);
        };
        return error.MissingInputs;
    }
    pub fn run(self: *Decoder, inputs: []const Tensor, a: std.mem.Allocator, control: ?Control) ![]Tensor {
        if (self.failed) return error.DecoderRequestFailed;
        self.control = control;
        return self.source.runCachedDecoder(self, inputs, a, control) catch |err| {
            self.failed = true;
            return err;
        };
    }
    pub fn text(self: *Decoder, history: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (self.failed) return error.DecoderRequestFailed;
        if (history.len == 0 or history.len > self.limit or (self.position != 0 and history.len != self.position + 1)) return error.InvalidResidentDecodePosition;
        errdefer self.failed = true;
        const ids = if (self.position == 0) history else history[history.len - 1 ..];
        const rows = if (self.position == 0) try capacityFor(ids.len, self.config.max_position_embeddings) else 1;
        // These copies precede per-step admission, so retain their real host
        // charge until the complete synchronous invocation has finished.
        var staging_bytes = try std.math.mul(usize, rows, 2 * @sizeOf(i64));
        if (self.axis_positions and self.position != 0) staging_bytes = try std.math.add(usize, staging_bytes, 3 * @sizeOf(i64));
        if (self.position == 0) staging_bytes = try std.math.add(usize, staging_bytes, @sizeOf(i64));
        var staging = try self.source.withAdmissionControl(control).admitRetainedMedia(staging_bytes, 0);
        defer staging.deinit();
        const padded = if (rows != ids.len) try a.alloc(i64, rows) else null;
        defer if (padded) |values| a.free(values);
        if (padded) |values| {
            @memcpy(values[0..ids.len], ids);
            @memset(values[ids.len..], 0);
        }
        var input = try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(rows) }, padded orelse ids);
        defer input.deinit();
        var logits_row: ?Tensor = if (self.position == 0) try Tensor.initInt64(a, "logits_row", &.{1}, &.{@intCast(ids.len - 1)}) else null;
        defer if (logits_row) |*tensor| tensor.deinit();
        var positions: ?Tensor = null;
        defer if (positions) |*tensor| tensor.deinit();
        if (self.axis_positions and self.position != 0) {
            const position = try std.math.add(i64, @intCast(self.position), self.position_delta);
            positions = try Tensor.initInt64(a, "position_ids", &.{ 3, 1 }, &.{ position, position, position });
        }
        var declared: [3]Tensor = undefined;
        declared[0] = input;
        var count: usize = 1;
        if (positions) |tensor| {
            declared[count] = tensor;
            count += 1;
        }
        if (logits_row) |tensor| {
            declared[count] = tensor;
            count += 1;
        }
        const outputs = try self.run(declared[0..count], a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        return a.dupe(f32, outputs[0].asFloat32());
    }
};

/// Bound padding while sharing one decode executable across nearby prompt and
/// output limits. The logical request limit remains independent of this bank.
pub fn capacityFor(requested: usize, maximum: usize) !usize {
    if (requested == 0 or requested > maximum or requested > 16_777_216) return error.InvalidResidentDecodePosition;
    const quantum = 64;
    return @min(maximum, try std.math.mul(usize, try std.math.divCeil(usize, requested, quantum), quantum));
}

test "resident decoder capacity buckets share programs without expanding logical limits" {
    try std.testing.expectEqual(@as(usize, 64), try capacityFor(8, 1024));
    try std.testing.expectEqual(try capacityFor(8, 1024), try capacityFor(63, 1024));
    try std.testing.expectEqual(@as(usize, 192), try capacityFor(129, 1024));
    try std.testing.expectEqual(@as(usize, 200), try capacityFor(193, 200));
    try std.testing.expectError(error.InvalidResidentDecodePosition, capacityFor(0, 1024));
    try std.testing.expectError(error.InvalidResidentDecodePosition, capacityFor(1025, 1024));
}
