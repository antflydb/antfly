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

/// Capability selection happens before allocation or execution. Automatic mode
/// preserves the ordinary PJRT route for unsupported state layouts; explicit
/// enablement fails rather than hiding an execution failure with a replay.
pub fn select(source: Session, requested: ?bool) !bool {
    if (requested == false) return false;
    const supported = enabled and source.backend() == .pjrt and source.vtable.runCachedDecoder != null and
        (if (source.model_contract.decoder_config) |cfg| supports(cfg.*) else false);
    if (requested == true and !supported) return error.UnsupportedResidentDecode;
    return supported;
}

pub fn supports(cfg: Config) bool {
    if (cfg.position_encoding != .rope or cfg.gemma4_mtp_assistant or cfg.usesMoe() or
        cfg.deepseek_v4_hc_mult != 0 or cfg.deepseek_v4_kv_lora_rank != 0 or
        cfg.num_kv_shared_layers > cfg.num_hidden_layers or cfg.num_hidden_layers == 0 or cfg.num_attention_heads == 0 or cfg.hidden_size == 0 or
        cfg.vocab_size == 0 or cfg.max_position_embeddings == 0) return false;
    if (cfg.qwen35_has_linear_attention and (cfg.qwen35_linear_conv_kernel_dim == 0 or
        cfg.qwen35_linear_key_head_dim == 0 or cfg.qwen35_linear_value_head_dim == 0 or
        cfg.qwen35_linear_num_key_heads == 0 or cfg.qwen35_linear_num_value_heads == 0 or
        cfg.qwen35_linear_num_value_heads % cfg.qwen35_linear_num_key_heads != 0)) return false;
    for (0..cfg.num_hidden_layers) |layer| {
        if (cfg.layerUsesQwen35LinearAttention(layer)) continue;
        const geometry = cfg.kvDonorGeometry(layer) catch return false;
        const rotary = cfg.layerRopeDim(layer);
        if (geometry.donor_layer > layer or geometry.num_kv_heads == 0 or geometry.head_dim == 0 or
            cfg.num_attention_heads % geometry.num_kv_heads != 0 or rotary == 0 or rotary % 2 != 0 or rotary > geometry.head_dim) return false;
    }
    return true;
}

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

    /// Decline optional caching only before provider allocation/execution. A
    /// short request cannot amortize prefill padding, and a denied cache bank
    /// must not prevent an otherwise admissible full-prefix PJRT request.
    pub fn initForRequest(a: std.mem.Allocator, source: Session, requested: ?bool, limit: usize, max_tokens: usize, control: ?Control) !?Decoder {
        if (requested == null and max_tokens <= 1) return null;
        if (!try select(source, requested)) return null;
        return init(a, source, limit, control) catch |err| {
            if (requested == null and err == error.ResourceLimitExceeded) return null;
            return err;
        };
    }

    pub fn init(a: std.mem.Allocator, source: Session, limit: usize, control: ?Control) !Decoder {
        if (control) |active| try active.check();
        const cfg = (source.model_contract.decoder_config orelse return error.UnsupportedResidentDecode).*;
        if (!try select(source, true)) return error.UnsupportedResidentDecode;
        const capacity = try capacityFor(limit, cfg.max_position_embeddings);
        var elements: usize = 0;
        var banks: usize = 0;
        for (0..cfg.num_hidden_layers) |layer| {
            if (!cfg.layerUsesQwen35LinearAttention(layer) and cfg.layerSharesKv(layer)) continue;
            banks = try std.math.add(usize, banks, 2);
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
        // Two non-donated generations coexist through publication. Initial
        // state is produced on device; the host retains only buffer headers.
        const header_size = if (comptime enabled) @sizeOf(@import("pjrt").pjrt.Buffer) else @sizeOf(usize) * 4;
        const headers = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, banks, 3), 4), header_size);
        const reservation = try source.withAdmissionControl(control).admitRetainedState(try std.math.add(usize, headers, 2 * @sizeOf(f32)), try std.math.add(usize, try std.math.mul(usize, bytes, 2), 2 * @sizeOf(f32)));
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

test "resident decoder capability rejects unsupported state layouts before execution" {
    var cfg = Config{ .position_encoding = .rope };
    try std.testing.expect(supports(cfg));
    cfg.position_encoding = .absolute;
    try std.testing.expect(!supports(cfg));
    cfg.position_encoding = .alibi;
    try std.testing.expect(!supports(cfg));
    cfg.position_encoding = .rope;
    cfg.gemma4_mtp_assistant = true;
    try std.testing.expect(!supports(cfg));
    cfg.gemma4_mtp_assistant = false;
    cfg.deepseek_v4_kv_lora_rank = 64;
    try std.testing.expect(!supports(cfg));
    cfg.deepseek_v4_kv_lora_rank = 0;
    cfg.num_key_value_heads = 7;
    try std.testing.expect(!supports(cfg));
    cfg.num_key_value_heads = 0;
    cfg.num_kv_shared_layers = cfg.num_hidden_layers + 1;
    try std.testing.expect(!supports(cfg));
}
