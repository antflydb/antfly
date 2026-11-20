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

//! Explicit request cache, outside loaded-model caches and execution lanes.
//! The reservation pins its source model until all provider buffers retire.
const std = @import("std");
const enabled = @import("build_options").enable_pjrt;
const Resident = if (enabled) @import("../graph/pjrt_decoder_state.zig").State else opaque {};
const Session = @import("../backends/session.zig").Session;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Config = @import("../models/gpt.zig").Config;
const DecoderCache = @import("../backends/session.zig").ModelContract.DecoderCache;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

/// Capability selection happens before allocation or execution. Automatic mode
/// preserves the ordinary PJRT route for unsupported state layouts; explicit
/// enablement fails rather than hiding an execution failure with a replay.
pub fn select(source: Session, requested: ?bool) !bool {
    if (requested == false) return false;
    const supported = enabled and source.backend() == .pjrt and source.vtable.runCachedDecoder != null and
        (if (source.model_contract.decoder_config) |cfg| supports(cfg.*) else false);
    if (requested == true and !supported) return error.UnsupportedResidentDecode;
    if (supported and requested == null and !source.model_contract.decoder_cache.automatic) return false;
    return supported;
}

/// Qualification follows immutable arithmetic policy and survives session wrappers.
pub fn profileFor(profile: anytype) DecoderCache {
    return .{ .automatic = profile.precision == .f32 or
        ((profile.platform == .cpu or profile.platform == .cuda) and profile.residual_storage == .f32) };
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
    encoder_decoder_config: ?@import("../models/whisper.zig").Config = null,
    encoder_rows: usize = 0,
    encoder_identity: ?[*]const u8 = null,
    encoder_mask_digest: u64 = 0,
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
    /// Same request owner and publication protocol for learned-position
    /// encoder/decoder models. Unknown arithmetic keeps ordinary execution.
    pub fn initEncoderDecoder(a: std.mem.Allocator, source: Session, encoder_rows: usize, requested: ?bool, control: ?Control) !?Decoder {
        if (requested == false) return null;
        const supported = enabled and source.backend() == .pjrt and source.vtable.runCachedDecoder != null and source.model_contract.encoder_decoder_config != null and source.model_contract.decoder_cache.automatic;
        if (!supported) return if (requested == true) error.UnsupportedResidentDecode else null;
        const cfg = (source.model_contract.encoder_decoder_config orelse return null).*;
        if (control) |active| try active.check();
        if (encoder_rows == 0 or encoder_rows > cfg.max_source_positions or cfg.decoder_layers == 0 or cfg.d_model == 0 or cfg.max_target_positions == 0) return error.InvalidModelConfig;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, cfg.decoder_layers, try std.math.mul(usize, try std.math.add(usize, cfg.max_target_positions, encoder_rows), cfg.d_model)), 2 * @sizeOf(f32));
        const banks = try std.math.mul(usize, cfg.decoder_layers, 4);
        const header_size = if (comptime enabled) @sizeOf(@import("pjrt").pjrt.Buffer) else @sizeOf(usize) * 4;
        const headers = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, banks, 3), 4), header_size);
        const reservation = source.withAdmissionControl(control).admitRetainedState(try std.math.add(usize, headers, 2 * @sizeOf(f32)), try std.math.add(usize, try std.math.mul(usize, bytes, 2), 2 * @sizeOf(f32))) catch |err| {
            if (requested == null and err == error.ResourceLimitExceeded) return null;
            return err;
        };
        return .{ .allocator = a, .source = source, .config = .{}, .encoder_decoder_config = cfg, .encoder_rows = encoder_rows, .capacity = cfg.max_target_positions, .limit = cfg.max_target_positions, .bank_bytes = bytes, .reservation = reservation, .control = control };
    }

    pub fn encoderDecoderText(self: *Decoder, history: []const i64, encoder: Tensor, mask: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (self.failed) return error.DecoderRequestFailed;
        const cfg = self.encoder_decoder_config orelse return error.UnsupportedResidentDecode;
        if (history.len == 0 or history.len > self.limit or (self.position != 0 and history.len <= self.position) or mask.len != self.encoder_rows) return error.InvalidResidentDecodePosition;
        errdefer self.failed = true;
        const mask_digest = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(mask));
        if (self.encoder_identity) |identity| {
            if (identity != encoder.data.ptr or mask_digest != self.encoder_mask_digest) return error.ImmutableEncoderContractChanged;
        } else {
            self.encoder_identity = encoder.data.ptr;
            self.encoder_mask_digest = mask_digest;
        }
        // Language detection can expose several already-known forced tokens.
        // Submit the complete new suffix; never drop those intermediate rows.
        const ids = history[self.position..];
        var staging = try self.source.withAdmissionControl(control).admitRetainedMedia(try std.math.add(usize, try std.math.mul(usize, ids.len, 3 * @sizeOf(i64)), try std.math.add(usize, try std.math.mul(usize, mask.len, @sizeOf(i64)), 6 * @sizeOf(i64))), 0);
        defer staging.deinit();
        const positions = try a.alloc(i64, ids.len);
        defer a.free(positions);
        for (positions, 0..) |*position, index| position.* = @intCast(self.position + index);
        var input = try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(ids.len) }, ids);
        defer input.deinit();
        var pos = try Tensor.initInt64(a, "position_ids", input.shape, positions);
        defer pos.deinit();
        var attention = try Tensor.initInt64(a, "encoder_attention_mask", &.{ 1, @intCast(mask.len) }, mask);
        defer attention.deinit();
        const hidden = encoder.borrowedView("encoder_hidden_states");
        if (hidden.dtype != .f32 or hidden.shape.len != 3 or hidden.shape[0] != 1 or hidden.shape[1] != self.encoder_rows or hidden.shape[2] != cfg.d_model) return error.InvalidInputShape;
        const outputs = try self.run(&.{ input, pos, hidden, attention }, a, control);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        if (outputs.len != 1 or outputs[0].asFloat32().len != cfg.vocab_size) return error.InvalidResidentDecoderOutput;
        return a.dupe(f32, outputs[0].asFloat32());
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
            if (self.encoder_decoder_config) |cfg| return .{ .sequence = rows, .output_bytes = try std.math.mul(usize, cfg.vocab_size, @sizeOf(f32)), .workspace_bytes = try @import("stage_workspace.zig").attentionFfn(1, rows, @max(self.capacity, self.encoder_rows), cfg.d_model, cfg.decoder_attention_heads, cfg.decoder_ffn_dim) };
            return @import("../architectures/frontends/decoder.zig").geometry(self.config, rows, self.capacity);
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

test "resident decoder automatic selection excludes unqualified reduced precision" {
    var profile = struct { platform: enum { cpu, cuda, other }, precision: @import("precision.zig").Policy, residual_storage: @import("precision.zig").ResidualStorage = .activation }{ .platform = .cpu, .precision = .bf16 };
    try std.testing.expect(!profileFor(profile).automatic);
    profile.residual_storage = .f32;
    try std.testing.expect(profileFor(profile).automatic);
    profile.residual_storage = .activation;
    profile.platform = .cuda;
    try std.testing.expect(!profileFor(profile).automatic);
    profile.residual_storage = .f32;
    try std.testing.expect(profileFor(profile).automatic);
    profile.precision = .f32;
    try std.testing.expect(profileFor(profile).automatic);
    profile.platform = .other;
    profile.precision = .bf16;
    try std.testing.expect(!profileFor(profile).automatic);
}

test "resident encoder decoder policy preserves explicit controls actual KV admission and cancellation" {
    if (comptime !enabled) return error.SkipZigTest;
    const Probe = struct {
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .pjrt;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.UnexpectedNumericalExecution;
        }
        fn controlled(raw: *anyopaque, values: []const Tensor, a: std.mem.Allocator, _: Control) anyerror![]Tensor {
            return run(raw, values, a);
        }
        fn cached(_: *anyopaque, _: *Decoder, _: []const Tensor, _: std.mem.Allocator, _: ?Control) anyerror![]Tensor {
            return error.UnexpectedNumericalExecution;
        }
        fn cancelled(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
        const table = Session.VTable{ .run = run, .runWithControl = controlled, .backend = backend, .inputInfo = info, .outputInfo = info, .close = close, .runCachedDecoder = cached };
    };
    const a = std.testing.allocator;
    var marker: u8 = 0;
    var domain = @import("../runtime/tier/memory.zig").AdmissionController{};
    const cfg = @import("testing/incremental_encoder_decoder.zig").config;
    var source = Session{ .ptr = &marker, .vtable = &Probe.table, .model_contract = .{ .encoder_decoder_config = &cfg, .decoder_cache = .{ .automatic = true } }, .run_admission = .{ .controller = &domain, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, false, null)) == null);
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    try std.testing.expectError(error.Cancelled, Decoder.initEncoderDecoder(a, source, 2, true, .{ .check_fn = Probe.cancelled }));
    var decoder = (try Decoder.initEncoderDecoder(a, source, 2, true, .{ .native_call_policy = .in_process })).?;
    // One layer, two banks of K+V, (8 self + 2 encoder) rows at width 4,
    // plus the two request cursors: 2 * 2 * 10 * 4 * 4 + 8 bytes.
    try std.testing.expectEqual(@as(usize, 648), domain.snapshot().host_kv_bytes);
    decoder.deinit();
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.kv_limit_bytes = 647;
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, null, null)) == null);
    try std.testing.expectError(error.ResourceLimitExceeded, Decoder.initEncoderDecoder(a, source, 2, true, null));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.model_contract.decoder_cache.automatic = false;
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, null, null)) == null);
    try std.testing.expectError(error.UnsupportedResidentDecode, Decoder.initEncoderDecoder(a, source, 2, true, null));
}

test "resident encoder decoder accepts forced prompt suffixes and rejects immutable changes" {
    if (comptime !enabled) return error.SkipZigTest;
    const fixture = @import("testing/incremental_encoder_decoder.zig");
    const Probe = struct {
        calls: usize = 0,
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .pjrt;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.UnexpectedNumericalExecution;
        }
        fn controlled(raw: *anyopaque, inputs: []const Tensor, a: std.mem.Allocator, _: Control) anyerror![]Tensor {
            return run(raw, inputs, a);
        }
        fn cached(raw: *anyopaque, state: *Decoder, inputs: []const Tensor, a: std.mem.Allocator, _: ?Control) anyerror![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const starts = [_]usize{ 0, 1, 3 };
            const lengths = [_]usize{ 1, 2, 1 };
            try std.testing.expectEqual(starts[self.calls], state.position);
            try std.testing.expectEqual(lengths[self.calls], inputs[0].asInt64().len);
            for (inputs[0].asInt64(), inputs[1].asInt64(), 0..) |token, position, index| {
                try std.testing.expectEqual(@as(i64, @intCast(state.position + index)), token);
                try std.testing.expectEqual(token, position);
            }
            const outputs = try a.alloc(Tensor, 1);
            errdefer a.free(outputs);
            outputs[0] = try Tensor.initFloat32(a, "logits", &.{ 1, fixture.config.vocab_size }, &(@as([fixture.config.vocab_size]f32, @splat(0))));
            state.position += lengths[self.calls];
            self.calls += 1;
            return outputs;
        }
    };
    const a = std.testing.allocator;
    var probe = Probe{};
    const session = Session{ .ptr = &probe, .vtable = &.{ .run = Probe.run, .runWithControl = Probe.controlled, .backend = Probe.backend, .inputInfo = Probe.info, .outputInfo = Probe.info, .close = Probe.close, .runCachedDecoder = Probe.cached }, .model_contract = .{ .encoder_decoder_config = &fixture.config, .decoder_cache = .{ .automatic = true } } };
    var decoder = (try Decoder.initEncoderDecoder(a, session, 2, true, .{ .native_call_policy = .in_process })).?;
    defer decoder.deinit();
    var hidden = try Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 2, 4 }, &.{ 0, 0, 0, 0, 0, 0, 0, 0 });
    defer hidden.deinit();
    const history = [_]i64{ 0, 1, 2, 3, 4 };
    for ([_]usize{ 1, 3, 4 }) |length| {
        const logits = try decoder.encoderDecoderText(history[0..length], hidden, &.{ 1, 1 }, a, null);
        a.free(logits);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectError(error.InvalidResidentDecodePosition, decoder.encoderDecoderText(history[0..4], hidden, &.{ 1, 1 }, a, null));
    try std.testing.expectError(error.ImmutableEncoderContractChanged, decoder.encoderDecoderText(&history, hidden, &.{ 1, 0 }, a, null));
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expect(decoder.failed);
}
