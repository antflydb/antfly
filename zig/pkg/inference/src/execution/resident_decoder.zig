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
    // CUDA BF16's deterministic resident schedule has sampled accuracy coverage.
    // Keep automatic selection gated until model and performance qualification
    // establishes where its fixed-bank work amortizes ordinary execution.
    const precision = @import("precision.zig");
    const stable_cuda = profile.platform == .cuda and profile.precision == .bf16 and profile.residual_storage == .f32 and
        precision.residentGemm(true, profile.precision, profile.gemm) == .vendor_deterministic;
    return .{
        .automatic = profile.precision == .f32 or (profile.platform == .cpu and profile.residual_storage == .f32),
        .max_prefill_query_rows = if (profile.precision == .f32 or stable_cuda) 512 else 0,
        .prefer_whole_prefill = profile.precision == .f32,
    };
}

/// Recurrent state keeps its existing execution shape until its multi-query
/// publication has separate qualification. This limit is a capability hint,
/// never permission to reinterpret a recurrent bank as ordinary KV.
fn causalQueryLimit(source: Session) usize {
    const cfg = source.model_contract.decoder_config orelse return 0;
    if (cfg.qwen35_has_linear_attention) return 0;
    return source.model_contract.decoder_cache.max_prefill_query_rows;
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
    /// Successfully published causal prefill queries, excluding decode steps.
    prefill_steps: usize = 0,
    prefill_head_skips: usize = 0,
    prefill_only: bool = false,
    axis_positions: bool = false,
    position_delta: i64 = 0,
    failed: bool = false,
    control: ?Control = null,
    reservation: @import("../backends/session.zig").RunPermit,

    /// Whole-prefix semantics (for example bidirectional media) cannot use a
    /// causal chunk fallback. The frontend supplies its complete host overlap.
    pub const WholePrefill = struct { rows: usize, input_bytes: usize, host_staging_bytes: usize };

    pub const RequestGeometry = struct {
        limit: usize,
        prompt_rows: usize,
        max_tokens: usize,
        query_rows: usize = 0,
        whole_prefill: ?WholePrefill = null,
    };

    /// Query bounds and actual prompt rows belong to this request. They never
    /// mutate model policy or infer a prompt length from a clipped context cap.
    pub fn initForRequest(a: std.mem.Allocator, source: Session, requested: ?bool, request: RequestGeometry, control: ?Control) !?Decoder {
        if (control) |active| try active.check();
        if (request.prompt_rows == 0 or request.prompt_rows > request.limit) return error.InvalidResidentDecodePosition;
        var bounded = source;
        const explicit_chunks = request.query_rows != 0 and request.prompt_rows > request.query_rows;
        if (request.query_rows != 0) {
            const cfg = source.model_contract.decoder_config orelse return error.UnsupportedResidentDecode;
            if (request.query_rows > cfg.max_position_embeddings) return error.InvalidPrefillQueryLimit;
            if (explicit_chunks and cfg.qwen35_has_linear_attention) return error.UnsupportedResidentPrefillChunking;
            bounded.model_contract.decoder_cache.max_prefill_query_rows = request.query_rows;
        }
        if (request.whole_prefill != null) bounded.model_contract.decoder_cache.max_prefill_query_rows = 0;
        const query_bound = causalQueryLimit(bounded);
        const needs_chunks = query_bound != 0 and request.prompt_rows > query_bound;
        if (!try select(bounded, requested)) {
            if (explicit_chunks) return error.UnsupportedResidentPrefillChunking;
            return null;
        }
        if (requested == null and (request.max_tokens == 0 or (request.max_tokens == 1 and !needs_chunks))) return null;
        // Cached padding is a property of this selected path. Automatic mode
        // may use ordinary whole-prefix execution instead; its caller validates
        // that path's actual geometry before submitting numerical work.
        if (request.whole_prefill) |whole| if (request.query_rows != 0 and whole.rows > request.query_rows) {
            if (requested == true or request.prompt_rows > request.query_rows) return error.UnsupportedResidentPrefillChunking;
            return null;
        };
        // An explicit bound requiring state cannot fall back to a larger query.
        // Preserve the real quota/pressure error instead of relabeling it as an
        // unsupported feature or replaying a whole-prefix operation.
        var decoder = (try initWithRequest(a, bounded, if (explicit_chunks) true else requested, request.limit, control)) orelse return null;
        // Construction has funded metadata only; no provider state exists.
        errdefer decoder.reservation.deinit();
        if (request.whole_prefill) |whole| {
            if (whole.rows < request.prompt_rows or whole.rows > decoder.capacity) return error.InvalidPrefillQueryLimit;
            if (!try decoder.prefillAdmitted(whole, requested, control)) {
                decoder.reservation.deinit();
                return null;
            }
        }
        if (request.query_rows == 0 and needs_chunks and bounded.model_contract.decoder_cache.prefer_whole_prefill and
            try decoder.wholePrefillAdmitted(request.prompt_rows, control))
        {
            if (requested == null and request.max_tokens == 1) {
                decoder.reservation.deinit();
                return null;
            }
            decoder.source.model_contract.decoder_cache.max_prefill_query_rows = 0;
        }
        return decoder;
    }

    /// A shape-only speculative lease preserves the whole-prefix matrix schedule
    /// when its complete peak fits beside the already charged banks. It creates
    /// no provider objects, grants no future credit, and submits no numerical
    /// work. Pressure keeps bounded queries; cancellation and real errors escape.
    fn wholePrefillAdmitted(self: *const Decoder, prompt_rows: usize, control: ?Control) !bool {
        if (control) |active| try active.check();
        // An unmanaged source has no evidence that a larger query fits.
        if (self.source.run_admission == null) return false;
        const rows = try prefillRowsFor(prompt_rows, self.config.max_position_embeddings);
        const staging_bytes = try std.math.add(usize, try std.math.mul(usize, rows, 2 * @sizeOf(i64)), @sizeOf(i64));
        return self.prefillAdmitted(.{ .rows = rows, .input_bytes = try std.math.add(usize, try std.math.mul(usize, rows, @sizeOf(i64)), @sizeOf(i64)), .host_staging_bytes = staging_bytes }, null, control);
    }

    fn prefillAdmitted(self: *const Decoder, whole: WholePrefill, requested: ?bool, control: ?Control) !bool {
        if (control) |active| try active.check();
        if (self.source.run_admission == null) return requested == true;
        const stage_geometry = try @import("../architectures/frontends/decoder.zig").geometry(self.config, whole.rows, self.capacity);
        const admitted = self.source.withAdmissionControl(control);
        const Memory = @import("../runtime/tier/memory.zig");
        var capture = Memory.AdmissionFailure.Capture.init(if (admitted.run_admission) |policy| policy.failure else null);
        var permit = admitted.withAdmissionFailure(capture.sink()).admit(.{
            .sequence = whole.rows,
            .input_bytes = whole.input_bytes,
            .output_bytes = stage_geometry.output_bytes,
            .workspace_bytes = stage_geometry.workspace_bytes,
            .host_preprocess_bytes = whole.host_staging_bytes,
        }) catch |err| {
            if (declineOptionalAdmission(requested, err)) return false;
            capture.publish(err);
            return err;
        };
        defer permit.deinit();
        if (control) |active| try active.check();
        return true;
    }

    pub fn init(a: std.mem.Allocator, source: Session, limit: usize, control: ?Control) !Decoder {
        return (try initWithRequest(a, source, true, limit, control)).?;
    }

    fn initWithRequest(a: std.mem.Allocator, source: Session, requested: ?bool, limit: usize, control: ?Control) !?Decoder {
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
        const reservation = (try reserveState(source, requested, try std.math.add(usize, headers, 2 * @sizeOf(f32)), try std.math.add(usize, try std.math.mul(usize, bytes, 2), 2 * @sizeOf(f32)), control)) orelse return null;
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
        const reservation = (try reserveState(source, requested, try std.math.add(usize, headers, 2 * @sizeOf(f32)), try std.math.add(usize, try std.math.mul(usize, bytes, 2), 2 * @sizeOf(f32)), control)) orelse return null;
        return .{ .allocator = a, .source = source, .config = .{}, .encoder_decoder_config = cfg, .encoder_rows = encoder_rows, .capacity = cfg.max_target_positions, .limit = cfg.max_target_positions, .bank_bytes = bytes, .reservation = reservation, .control = control };
    }

    pub fn encoderDecoderSuffix(self: *Decoder, suffix: @import("decoder_position.zig").Suffix, encoder: Tensor, mask: []const i64, a: std.mem.Allocator, control: ?Control) ![]f32 {
        if (self.failed) return error.DecoderRequestFailed;
        const cfg = self.encoder_decoder_config orelse return error.UnsupportedResidentDecode;
        const end = suffix.endAt(self.position) catch return error.InvalidResidentDecodePosition;
        if (end > self.limit or mask.len != self.encoder_rows) return error.InvalidResidentDecodePosition;
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
        const ids = suffix.tokens;
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
            return @import("../architectures/frontends/decoder.zig").geometryForOutput(self.config, rows, self.capacity, if (self.prefill_only) .hidden else .logits);
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
        const initial = self.position == 0;
        const chunk = canonicalQueryLimit(causalQueryLimit(self.source));
        if (initial and chunk != 0) {
            var end = @min(chunk, history.len);
            while (end < history.len) {
                const intermediate = try self.appendText(history[0..end], false, a, control);
                a.free(intermediate);
                self.prefill_steps += 1;
                self.prefill_head_skips += 1;
                end += @min(chunk, history.len - end);
            }
        }
        const logits = try self.appendText(history, true, a, control);
        if (initial) self.prefill_steps += 1;
        return logits;
    }

    /// Internal append consumes every new query row. Public text decoding still
    /// accepts one token after prefill; only this owner schedules causal chunks.
    fn appendText(self: *Decoder, history: []const i64, publish_logits: bool, a: std.mem.Allocator, control: ?Control) ![]f32 {
        self.prefill_only = !publish_logits;
        defer self.prefill_only = false;
        const ids = history[self.position..];
        const rows = try boundedPrefillRows(ids.len, self.capacity - self.position, self.config.max_position_embeddings, causalQueryLimit(self.source));
        // These copies precede per-step admission, so retain their real host
        // charge until the complete synchronous invocation has finished.
        var staging_bytes = try std.math.mul(usize, rows, 2 * @sizeOf(i64));
        if (self.axis_positions and self.position != 0) staging_bytes = try std.math.add(usize, staging_bytes, try std.math.mul(usize, rows, 6 * @sizeOf(i64)));
        if (self.position == 0 or ids.len > 1) staging_bytes = try std.math.add(usize, staging_bytes, @sizeOf(i64));
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
        var logits_row: ?Tensor = if (self.position == 0 or ids.len > 1) try Tensor.initInt64(a, "logits_row", &.{1}, &.{@intCast(ids.len - 1)}) else null;
        defer if (logits_row) |*tensor| tensor.deinit();
        var positions: ?Tensor = null;
        defer if (positions) |*tensor| tensor.deinit();
        if (self.axis_positions and self.position != 0) {
            const values = try a.alloc(i64, try std.math.mul(usize, rows, 3));
            defer a.free(values);
            for (0..3) |axis| for (0..rows) |row| {
                values[axis * rows + row] = try std.math.add(i64, @intCast(self.position + row), self.position_delta);
            };
            positions = try Tensor.initInt64(a, "position_ids", &.{ 3, @intCast(rows) }, values);
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

/// Share the optional transaction between decoder families. Speculative
/// denial details never escape a successful compatibility fallback, and the
/// retained permit never keeps the stack-owned diagnostic capture.
fn reserveState(source: Session, requested: ?bool, host_bytes: usize, state_bytes: usize, control: ?Control) !?@import("../backends/session.zig").RunPermit {
    const admitted = source.withAdmissionControl(control);
    if (requested != null) return try admitted.admitRetainedState(host_bytes, state_bytes);
    const Memory = @import("../runtime/tier/memory.zig");
    var capture = Memory.AdmissionFailure.Capture.init(if (admitted.run_admission) |policy| policy.failure else null);
    var permit = admitted.withAdmissionFailure(capture.sink()).admitRetainedState(host_bytes, state_bytes) catch |err| {
        if (declineOptionalAdmission(requested, err)) return null;
        capture.publish(err);
        return err;
    };
    permit.session = admitted;
    return permit;
}

/// Only pre-execution quota/pressure denials can decline optional state. An
/// explicit request, cancellation, provider failure or allocation failure keeps
/// its original error; no submitted numerical work is replayed.
fn declineOptionalAdmission(requested: ?bool, err: anyerror) bool {
    if (requested != null) return false;
    return switch (err) {
        error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable, error.MemoryBudgetExceeded => true,
        else => false,
    };
}

/// Query geometry is independent of KV capacity. Small geometric buckets bound
/// padded work below twice the logical prompt, while the existing 64-row bank
/// buckets continue sharing decode programs. At most six extra small prefill
/// shapes enter the existing bounded specialization cache.
pub fn prefillRowsFor(requested: usize, maximum: usize) !usize {
    const capacity = try capacityFor(requested, maximum);
    if (requested >= 64) return capacity;
    return @min(maximum, try std.math.ceilPowerOfTwo(usize, requested));
}

/// Reuse the existing tuned query buckets below a physical ceiling instead of
/// inventing a specialization for every caller limit. Zero retains whole prefill.
fn canonicalQueryLimit(limit: usize) usize {
    if (limit == 0) return 0;
    return if (limit < 64) std.math.floorPowerOfTwo(usize, limit) else limit - limit % 64;
}

/// A physical query never exceeds the declared bound, even for non-aligned
/// caller limits. Chunk scheduling selects canonical buckets beneath it.
pub fn boundedPrefillRows(requested: usize, remaining: usize, maximum: usize, limit: usize) !usize {
    var rows = @min(try prefillRowsFor(requested, maximum), remaining);
    if (limit != 0) rows = @min(rows, limit);
    if (rows < requested) return error.InvalidPrefillQueryLimit;
    return rows;
}

test "resident decoder physical query buckets honor non aligned limits" {
    try std.testing.expectEqual(@as(usize, 0), canonicalQueryLimit(0));
    try std.testing.expectEqual(@as(usize, 32), canonicalQueryLimit(33));
    try std.testing.expectEqual(@as(usize, 64), canonicalQueryLimit(65));
    try std.testing.expectEqual(@as(usize, 960), canonicalQueryLimit(1023));
    for (1..4097) |limit| {
        const selected = canonicalQueryLimit(limit);
        try std.testing.expect(selected != 0 and selected <= limit);
        try std.testing.expect(selected < 64 or selected % 64 == 0);
        try std.testing.expectEqual(selected, try boundedPrefillRows(selected, 8192, 8192, limit));
    }

    for (1..130) |limit| for (1..limit + 1) |requested| {
        const rows = try boundedPrefillRows(requested, 256, 1024, limit);
        try std.testing.expect(rows >= requested and rows <= limit);
        try std.testing.expect(rows < 2 * requested);
    };
    try std.testing.expectEqual(@as(usize, 65), try boundedPrefillRows(65, 256, 1024, 65));
    try std.testing.expectEqual(@as(usize, 33), try boundedPrefillRows(33, 256, 1024, 33));
    try std.testing.expectEqual(@as(usize, 128), try boundedPrefillRows(65, 256, 1024, 0));
    try std.testing.expectError(error.InvalidPrefillQueryLimit, boundedPrefillRows(66, 256, 1024, 65));
}

test "resident decoder query buckets bound short prefill work independently of KV" {
    for (1..130) |rows| {
        const queries = try prefillRowsFor(rows, 1024);
        try std.testing.expect(queries >= rows);
        try std.testing.expect(queries < 2 * rows);
        try std.testing.expect(queries <= try capacityFor(rows, 1024));
        // Even a two-output request performs fewer query rows than two
        // full-prefix forwards: padded prefill + one cached query < n + n+1.
        try std.testing.expect(queries + 1 < 2 * rows + 1);
    }
    try std.testing.expectEqual(@as(usize, 1), try prefillRowsFor(1, 1024));
    try std.testing.expectEqual(@as(usize, 4), try prefillRowsFor(3, 1024));
    try std.testing.expectEqual(@as(usize, 33), try prefillRowsFor(33, 33));
    try std.testing.expectEqual(@as(usize, 192), try prefillRowsFor(129, 1024));
    try std.testing.expectError(error.InvalidResidentDecodePosition, prefillRowsFor(0, 1024));
    try std.testing.expectError(error.InvalidResidentDecodePosition, prefillRowsFor(33, 32));
}

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
    var profile = struct { platform: enum { cpu, cuda, other }, precision: @import("precision.zig").Policy, gemm: @import("precision.zig").Gemm = .provider_default, residual_storage: @import("precision.zig").ResidualStorage = .activation }{ .platform = .cpu, .precision = .bf16 };
    try std.testing.expect(!profileFor(profile).automatic);
    profile.residual_storage = .f32;
    try std.testing.expect(profileFor(profile).automatic);
    profile.residual_storage = .activation;
    profile.platform = .cuda;
    try std.testing.expect(!profileFor(profile).automatic);
    profile.residual_storage = .f32;
    try std.testing.expect(!profileFor(profile).automatic);
    try std.testing.expectEqual(@as(usize, 512), profileFor(profile).max_prefill_query_rows);
    profile.gemm = .vendor;
    try std.testing.expectEqual(@as(usize, 0), profileFor(profile).max_prefill_query_rows);
    profile.gemm = .provider_default;
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
        fn cancelFunded(raw: ?*anyopaque) anyerror!void {
            const controller: *@import("../runtime/tier/memory.zig").AdmissionController = @ptrCast(@alignCast(raw.?));
            if (controller.snapshot().hostTotalBytes() != 0) return error.Cancelled;
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
    // The optional bank is refused before provider execution. The ordinary
    // request remains admissible after transient pressure, and no lease leaks.
    domain.configureForcedRunDenialsForTesting(1);
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, null, null)) == null);
    domain.configureForcedRunDenialsForTesting(1);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Decoder.initEncoderDecoder(a, source, 2, true, null));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.kv_limit_bytes = 647;
    var failure: @import("../runtime/tier/memory.zig").AdmissionFailure = .{};
    const diagnosed = Control{ .admission_failure = &failure };
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, null, diagnosed)) == null);
    try std.testing.expect(failure.snapshot() == null);
    try std.testing.expectError(error.ResourceLimitExceeded, Decoder.initEncoderDecoder(a, source, 2, true, diagnosed));
    try std.testing.expect(failure.snapshot() != null);
    try std.testing.expect((try Decoder.initEncoderDecoder(a, source, 2, null, null)) == null);
    try std.testing.expectError(error.ResourceLimitExceeded, Decoder.initEncoderDecoder(a, source, 2, true, null));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.kv_limit_bytes = 0;
    const gpt = Config{ .position_encoding = .rope };
    source.model_contract.decoder_config = &gpt;
    domain.configureForcedRunDenialsForTesting(1);
    try std.testing.expect((try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, null)) == null);
    domain.configureForcedRunDenialsForTesting(1);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Decoder.initForRequest(a, source, true, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, null));
    try std.testing.expectError(error.Cancelled, Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, .{ .check_fn = Probe.cancelled }));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.kv_limit_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 2 }, null));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.kv_limit_bytes = 0;
    source.model_contract.decoder_cache.max_prefill_query_rows = 2;
    source.model_contract.decoder_cache.prefer_whole_prefill = true;
    var retained_header_bytes: usize = 0;
    {
        var selected = (try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, null)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 0), causalQueryLimit(selected.source));
        retained_header_bytes = domain.snapshot().host_scratch_bytes;
    }
    source.run_admission.?.limits.scratch_limit_bytes = retained_header_bytes + 1;
    var speculative_failure: @import("../runtime/tier/memory.zig").AdmissionFailure = .{};
    {
        var selected = (try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, .{ .admission_failure = &speculative_failure, .native_call_policy = .in_process })).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 2), causalQueryLimit(selected.source));
        try std.testing.expect(speculative_failure.snapshot() == null);
    }
    // A whole-prefix stage cannot use the causal chunk fallback. Optional
    // caching declines without quota/diagnostic leakage; explicit requests
    // preserve the original admission error before any provider submission.
    const whole = Decoder.WholePrefill{ .rows = 8, .input_bytes = 64, .host_staging_bytes = 128 };
    try std.testing.expect((try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .whole_prefill = whole }, .{ .admission_failure = &speculative_failure })) == null);
    try std.testing.expect(speculative_failure.snapshot() == null);
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    try std.testing.expectError(error.ResourceLimitExceeded, Decoder.initForRequest(a, source, true, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .whole_prefill = whole }, null));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    // The complete whole-prefix envelope fits without retained cache headers.
    // Adding automatic state must decline caching rather than reject that run.
    const shape = try @import("../architectures/frontends/decoder.zig").geometry(gpt, whole.rows, try capacityFor(16, gpt.max_position_embeddings));
    const ordinary_request = @import("../backends/session.zig").RunRequest{ .sequence = whole.rows, .input_bytes = whole.input_bytes, .output_bytes = shape.output_bytes, .workspace_bytes = shape.workspace_bytes, .host_preprocess_bytes = whole.host_staging_bytes };
    const required = (try source.run_admission.?.planRequest(ordinary_request, &.{})).required;
    source.run_admission.?.limits.scratch_limit_bytes = required.host_scratch_bytes;
    {
        var ordinary = try source.admit(ordinary_request);
        ordinary.deinit();
    }
    try std.testing.expect((try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .whole_prefill = whole }, null)) == null);
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    source.run_admission.?.limits.scratch_limit_bytes = 0;
    {
        var selected = (try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .whole_prefill = whole }, null)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 0), causalQueryLimit(selected.source));
    }
    try std.testing.expectError(error.Cancelled, Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .whole_prefill = whole }, .{ .ptr = &domain, .check_fn = Probe.cancelFunded }));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    try std.testing.expectError(error.UnsupportedResidentPrefillChunking, Decoder.initForRequest(a, source, true, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 2, .whole_prefill = whole }, null));
    const padded_whole: Decoder.WholePrefill = .{ .rows = 16, .input_bytes = whole.input_bytes, .host_staging_bytes = whole.host_staging_bytes };
    for ([_]?bool{ false, null }) |policy| {
        try std.testing.expect((try Decoder.initForRequest(a, source, policy, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 8, .whole_prefill = padded_whole }, null)) == null);
        try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    }
    try std.testing.expectError(error.UnsupportedResidentPrefillChunking, Decoder.initForRequest(a, source, true, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 8, .whole_prefill = padded_whole }, null));
    {
        var selected = (try Decoder.initForRequest(a, source, true, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 8, .whole_prefill = whole }, null)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 0), causalQueryLimit(selected.source));
    }

    {
        var selected = (try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8, .query_rows = 2 }, null)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 2), causalQueryLimit(selected.source));
    }
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    {
        var unmanaged = source;
        unmanaged.run_admission = null;
        var selected = (try Decoder.initForRequest(a, unmanaged, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, null)).?;
        defer selected.deinit();
        try std.testing.expectEqual(@as(usize, 2), causalQueryLimit(selected.source));
    }
    try std.testing.expectError(error.Cancelled, Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 8 }, .{ .ptr = &domain, .check_fn = Probe.cancelFunded }));
    try std.testing.expectEqual(@as(usize, 0), domain.snapshot().hostTotalBytes());
    try std.testing.expect((try Decoder.initForRequest(a, source, null, .{ .limit = 16, .prompt_rows = 8, .max_tokens = 1 }, .{})) == null);
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
        const logits = try decoder.encoderDecoderSuffix(.{ .position = decoder.position, .tokens = history[decoder.position..length] }, hidden, &.{ 1, 1 }, a, null);
        a.free(logits);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectError(error.InvalidResidentDecodePosition, decoder.encoderDecoderSuffix(.{ .position = 0, .tokens = history[0..4] }, hidden, &.{ 1, 1 }, a, null));
    try std.testing.expectError(error.ImmutableEncoderContractChanged, decoder.encoderDecoderSuffix(.{ .position = decoder.position, .tokens = history[decoder.position..] }, hidden, &.{ 1, 0 }, a, null));
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expect(decoder.failed);
}

test "resident decoder submits bounded physical chunks and consumes every valid row" {
    if (comptime !enabled) return error.SkipZigTest;
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
        fn cached(raw: *anyopaque, state: *Decoder, inputs: []const Tensor, a: std.mem.Allocator, _: ?Control) anyerror![]Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ids = inputs[0].asInt64();
            try std.testing.expect(ids.len <= 65);
            if (self.calls < 2) try std.testing.expectEqual(@as(usize, 64), ids.len);
            var valid = ids.len;
            for (inputs) |input| if (std.mem.eql(u8, input.name, "logits_row")) {
                valid = @intCast(input.asInt64()[0] + 1);
            };
            for (ids[0..valid], 0..) |value, i| try std.testing.expectEqual(@as(i64, @intCast(state.position + i)), value);
            self.calls += 1;
            state.position += valid;
            const outputs = try a.alloc(Tensor, 1);
            errdefer a.free(outputs);
            outputs[0] = try Tensor.initFloat32(a, "logits", &.{1}, &.{0});
            return outputs;
        }
        const table = Session.VTable{ .run = run, .backend = backend, .inputInfo = info, .outputInfo = info, .close = close, .runCachedDecoder = cached };
    };
    const a = std.testing.allocator;
    var probe = Probe{};
    const cfg = Config{ .hidden_size = 4, .intermediate_size = 8, .num_attention_heads = 1, .num_key_value_heads = 1, .num_hidden_layers = 1, .vocab_size = 256, .max_position_embeddings = 256, .position_encoding = .rope };
    const source = Session{ .ptr = &probe, .vtable = &Probe.table, .model_contract = .{ .decoder_config = &cfg } };
    const control = Control{ .native_call_policy = .in_process };
    var decoder = (try Decoder.initForRequest(a, source, true, .{ .limit = 200, .prompt_rows = 150, .max_tokens = 50, .query_rows = 65 }, control)).?;
    defer decoder.deinit();
    var ids: [151]i64 = undefined;
    for (&ids, 0..) |*v, i| v.* = @intCast(i);
    const prefill = try decoder.text(ids[0..150], a, control);
    defer a.free(prefill);
    try std.testing.expectEqual(@as(usize, 150), decoder.position);
    try std.testing.expectEqual(@as(usize, 3), decoder.prefill_steps);
    try std.testing.expectEqual(@as(usize, 2), decoder.prefill_head_skips);
    const decode = try decoder.text(&ids, a, control);
    defer a.free(decode);
    try std.testing.expectEqual(@as(usize, 151), decoder.position);
    try std.testing.expectEqual(@as(usize, 4), probe.calls);
}
