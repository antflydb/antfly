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

//! Encoder/decoder stage placement. Tasks keep token acceptance, stopping and
//! text publication; this owner keeps hidden activations in a shared context.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const session_mod = @import("../backends/session.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Incremental = @import("architecture_session.zig").IncrementalDecoderAdapter;

pub const EncoderOutputs = union(enum) {
    host: []backends.Tensor,
    resident: struct { values: session_mod.ResidentOutputs, shape: [3]i64, incremental_receiver: ?Incremental.Receiver = null },

    /// Validate the destination and publication before a consumer retains
    /// the encoder resources or detaches the producer's physical output.
    pub fn incrementalReceiverFor(self: *const EncoderOutputs, destination: backends.Session) !?Incremental.Receiver {
        const resident = switch (self.*) {
            .host => return null,
            .resident => |*value| value,
        };
        const receiver = resident.incremental_receiver orelse return null;
        if (receiver.destination_ptr != destination.ptr or receiver.destination_vtable != destination.vtable or resident.values.outputs.len != 1)
            return error.InvalidIncrementalReceiver;
        return receiver;
    }

    pub fn deinit(self: *EncoderOutputs, allocator: std.mem.Allocator) void {
        self.deinitWithCleanup(allocator, .{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *EncoderOutputs, allocator: std.mem.Allocator, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(allocator, .{ .boundary = boundary });
    }
    fn deinitWithCleanup(self: *EncoderOutputs, allocator: std.mem.Allocator, current: @import("cleanup_context.zig").Context) void {
        switch (self.*) {
            .host => |values| {
                for (values) |*value| value.deinit();
                allocator.free(values);
            },
            .resident => |*value| if (current.outside_execution) value.values.deinit() else value.values.deinitWithBoundary(current.boundary),
        }
    }
};

/// Qualification precedes encoder execution: a rejected pair takes its host
/// reference once. Merged-cache decoders have a separate stateful contract.
const PlacementReason = @import("diagnostics.zig").StagePlacement.Reason;

fn residentStageDecline(encoder: backends.Session, decoder: backends.Session, controlled: bool) ?PlacementReason {
    const source = encoder.shared_compute_context orelse return .no_source_context;
    if (decoder.shared_compute_context != source) return .foreign_context;
    if (encoder.vtable.runResident == null or decoder.vtable.runResidentInputs == null) return .missing_resident_entry;
    if (controlled and (encoder.vtable.runResidentWithControl == null or decoder.vtable.runResidentInputsWithControl == null)) return .missing_controlled_entry;
    if (source.stableBackend().vtable.sliceRows2D == null) return .missing_resident_row_selection;
    const outputs = encoder.outputInfo();
    if (outputs.len != 1 or outputs[0].dtype != .f32 or outputs[0].shape.len != 3) return .unsupported_encoder_signature;
    return null;
}

fn compatibleResidentEncoderShape(produced: []const i64, consumed: []const i64) bool {
    for (produced, consumed) |source, destination| {
        if (source > 0 and destination > 0 and source != destination) return false;
    }
    return true;
}

pub fn residentPairPlacement(encoder: backends.Session, decoder: backends.Session, controlled: bool) PlacementReason {
    if (residentStageDecline(encoder, decoder, controlled)) |reason| return reason;
    const outputs = encoder.outputInfo();
    const logits = decoder.outputInfo();
    if (logits.len != 1 or logits[0].dtype != .f32 or logits[0].shape.len != 3) return .unsupported_decoder_signature;
    const inputs = decoder.inputInfo();
    const signature = DecoderInputPlan.Signature.resolve(inputs) catch return .unsupported_decoder_signature;
    var tokens = false;
    var hidden = false;
    for (inputs, signature.roles[0..signature.count]) |input, role| {
        switch (role) {
            .input_ids, .encoder_attention_mask, .decoder_attention_mask => {
                if (input.dtype != .i64 or input.shape.len != 2) return .unsupported_decoder_signature;
                if (role == .input_ids) tokens = true;
            },
            .encoder_hidden_states => {
                if (input.dtype != .f32 or input.shape.len != 3) return .unsupported_decoder_signature;
                if (!compatibleResidentEncoderShape(outputs[0].shape, input.shape)) return .unsupported_decoder_signature;
                hidden = true;
            },
        }
    }
    return if (tokens and hidden) .resident_pair else .unsupported_decoder_signature;
}

fn residentMergedPlacement(encoder: backends.Session, decoder: backends.Session, controlled: bool) ?PlacementReason {
    const vocab = for (decoder.outputInfo()) |output| {
        if (std.mem.eql(u8, output.name, "logits") and output.shape.len == 3 and output.shape[2] > 0) {
            break std.math.cast(usize, output.shape[2]) orelse return null;
        }
    } else return null;
    if (!@import("seq2seq_runtime.zig").qualified(decoder, vocab)) return null;
    if (residentStageDecline(encoder, decoder, controlled)) |reason| return reason;
    const produced = encoder.outputInfo()[0].shape;
    for (decoder.inputInfo()) |input| {
        if (std.mem.eql(u8, input.name, "encoder_hidden_states")) {
            return if (compatibleResidentEncoderShape(produced, input.shape)) .resident_merged_decoder else .unsupported_decoder_signature;
        }
    }
    return .unsupported_decoder_signature;
}

/// The optional synchronous dispatch capability has the existing run method;
/// its task classification stays in its caller and is never stored here.
pub fn encode(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control) !EncoderOutputs {
    return encodeWithPermit(allocator, encoder, decoder, inputs, dispatch, control, null);
}

/// A preprocessing permit may transfer its lease into resident encoder outputs.
/// Host reference execution preserves the caller's existing permit lifetime.
pub fn encodeWithPermit(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit) !EncoderOutputs {
    return encodeOptions(allocator, encoder, decoder, inputs, dispatch, control, supplied, .resident);
}

/// An incremental consumer explicitly opts into its typed cache receiver. A
/// stateless full-prefix task must never inherit a native KV route implicitly.
pub fn encodeForIncrementalWithPermit(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit) !EncoderOutputs {
    return encodeOptions(allocator, encoder, decoder, inputs, dispatch, control, supplied, .incremental);
}

pub const EncoderIntent = enum { resident, incremental, vocabulary_row };

const EncoderRoute = struct {
    dispatcher: ?@import("tensor_dispatch.zig").Dispatch = null,
    incremental: ?Incremental.Receiver = null,
    placement: PlacementReason,

    fn isResident(self: EncoderRoute) bool {
        return self.placement == .resident_pair or self.placement == .resident_incremental or self.placement == .resident_merged_decoder;
    }
};
fn encoderRoute(encoder: backends.Session, decoder: backends.Session, dispatch: anytype, control: ?Control, intent: EncoderIntent) EncoderRoute {
    const stages = @import("tensor_dispatch.zig");
    if (intent == .vocabulary_row and outputVocabulary(decoder) == null) {
        return .{ .placement = .unknown_decoder_vocabulary };
    }
    const dispatcher = stages.stageDispatcher(dispatch);
    if (stages.hasDispatcher(dispatch) and (dispatcher == null or !dispatcher.?.supportsResident())) {
        return .{ .placement = .dispatcher_host_only };
    }
    if (intent == .incremental) {
        if (Incremental.qualify(encoder, decoder)) |receiver| {
            return .{ .dispatcher = dispatcher, .incremental = receiver, .placement = .resident_incremental };
        }
        if (residentMergedPlacement(encoder, decoder, control != null)) |placement| return .{ .dispatcher = dispatcher, .placement = placement };
    }
    const placement = residentPairPlacement(encoder, decoder, control != null);
    return .{ .dispatcher = dispatcher, .placement = placement };
}

/// The owning task calls this before its model/provider lane. Qualification is
/// shared with execution; host fallbacks allocate no resident metadata. A typed
/// dispatcher prepares its own exact execution, so its preprocessing permit is
/// left unchanged. Supplied execution itself only validates this prepayment.
pub fn prepareEncoderPermit(encoder: backends.Session, decoder: backends.Session, dispatch: anytype, control: ?Control, intent: EncoderIntent, permit: *session_mod.RunPermit) !void {
    if (permit.session.ptr != encoder.ptr or permit.session.vtable != encoder.vtable) return error.InvalidStagePermit;
    const selected = encoderRoute(encoder, decoder, dispatch, control, intent);
    if (selected.isResident() and selected.dispatcher == null) try permit.prepareResidentOutputRetirement();
}

fn encodeOptions(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit, intent: EncoderIntent) !EncoderOutputs {
    if (supplied) |permit| if (permit.session.ptr != encoder.ptr or permit.session.vtable != encoder.vtable) return error.InvalidStagePermit;
    const selected = encoderRoute(encoder, decoder, dispatch, control, intent);
    observePlacement(encoder, decoder, selected.placement);
    const incremental = selected.incremental;
    if (selected.isResident()) {
        var own_permit: ?session_mod.RunPermit = null;
        defer if (own_permit) |*owned| owned.deinit();
        const permit: ?*session_mod.RunPermit = if (selected.dispatcher != null) supplied else supplied orelse blk: {
            own_permit = try encoder.admitInputs(inputs, allocator, control);
            break :blk &own_permit.?;
        };
        if (selected.dispatcher == null) if (permit) |owner| {
            if (own_permit != null) try owner.prepareResidentOutputRetirement() else try owner.validateResidentOutputRetirement();
        };
        var values = if (selected.dispatcher) |dispatcher|
            (try dispatcher.runResident(allocator, encoder, permit, null, inputs, control)) orelse return error.UnexpectedResidentStageDecline
        else
            (try permit.?.runResidentWithControl(inputs, allocator, control)) orelse return error.UnexpectedResidentStageDecline;
        errdefer values.deinit();
        if (values.resource_lease == null) if (permit) |owner| {
            values.resource_lease = owner.lease;
            owner.lease = null;
        };
        const shape = try residentEncoderShape(allocator, encoder.outputInfo(), &values);
        return .{ .resident = .{ .values = values, .shape = shape, .incremental_receiver = incremental } };
    }
    return encodeHost(allocator, encoder, inputs, dispatch, control, supplied);
}

fn residentEncoderShape(allocator: std.mem.Allocator, outputs: []const backends.TensorInfo, values: *const session_mod.ResidentOutputs) ![3]i64 {
    if (values.outputs.len != 1) return error.InvalidEncoderOutput;
    const value = values.outputs[0];
    if (outputs.len == 1) {
        const declared = outputs[0].shape;
        if (declared.len == 3 and declared[0] == 1 and declared[1] > 0 and declared[2] > 0) {
            if ((try values.backend.tensorShapeMatches(value, declared)) orelse false) return declared[0..3].*;
        }
    }
    const actual = try values.backend.tensorShape(value, allocator);
    defer allocator.free(actual);
    if (actual.len != 3 or actual[0] != 1 or actual[1] <= 0 or actual[2] <= 0) return error.InvalidEncoderOutput;
    return actual[0..3].*;
}

fn observePlacement(encoder: backends.Session, decoder: backends.Session, reason: PlacementReason) void {
    const binding = encoder.invocation_diagnostics orelse return;
    if (!binding.diagnostics.isEnabled()) return;
    publishPlacement(binding.diagnostics, encoder.backend(), decoder.backend(), reason);
}

noinline fn publishPlacement(publisher: *@import("diagnostics.zig").Diagnostics, source: backends.BackendType, destination: backends.BackendType, reason: PlacementReason) void {
    publisher.publish(.{
        .scope = .session_preparation,
        .operation = .encoder,
        .stage_placement = .{ .source_backend = source, .destination_backend = destination, .reason = reason },
    });
}

fn encodeHost(allocator: std.mem.Allocator, encoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit) !EncoderOutputs {
    return .{ .host = try @import("tensor_dispatch.zig").run(allocator, encoder, supplied, null, inputs, control, dispatch) };
}

/// Full-prefix decoder ABI resolved once for a request. Token and encoder
/// mask slices are borrowed synchronously; the optional query mask grows only
/// when the next prefix exceeds the current capacity.
pub const DecoderInputPlan = struct {
    const Role = enum { input_ids, encoder_attention_mask, decoder_attention_mask, encoder_hidden_states };
    const Signature = struct {
        roles: [4]Role = undefined,
        count: usize,

        fn resolve(inputs: []const backends.TensorInfo) !Signature {
            var signature: Signature = .{ .count = inputs.len };
            if (inputs.len > signature.roles.len) return error.InvalidDecoderInput;
            var seen: std.EnumSet(Role) = .empty;
            for (inputs, signature.roles[0..signature.count]) |info, *role| {
                role.* = std.meta.stringToEnum(Role, info.name) orelse return error.InvalidDecoderInput;
                if (seen.contains(role.*)) return error.InvalidDecoderInput;
                seen.insert(role.*);
            }
            return signature;
        }
    };
    allocator: std.mem.Allocator,
    signature: Signature,
    query_mask: std.ArrayList(i64) = .empty,

    pub fn init(allocator: std.mem.Allocator, inputs: []const backends.TensorInfo) !DecoderInputPlan {
        return .{ .allocator = allocator, .signature = try Signature.resolve(inputs) };
    }

    pub fn deinit(self: *DecoderInputPlan) void {
        self.query_mask.deinit(self.allocator);
        self.* = undefined;
    }

    fn borrow(self: *const DecoderInputPlan, role: Role, shape: []const i64, data: []const i64) session_mod.StageInput {
        return .{ .host = .{
            .name = @tagName(role),
            .data = @constCast(std.mem.sliceAsBytes(data)),
            .shape = shape,
            .dtype = .i64,
            .allocator = self.allocator,
            .owns_data = false,
            .owns_shape = false,
        } };
    }

    /// Initialize output in place. Its shape arrays remain valid until the
    /// stage returns. No borrowed pointer escapes into the request plan.
    pub fn bind(self: *DecoderInputPlan, result: *DecoderInputs, tokens: []const i64, mask: []const i64, hidden: session_mod.StageInput) !void {
        result.* = .{
            .count = self.signature.count,
            .token_shape = .{ 1, std.math.cast(i64, tokens.len) orelse return error.ResourceLimitExceeded },
            .encoder_mask_shape = .{ 1, std.math.cast(i64, mask.len) orelse return error.ResourceLimitExceeded },
        };
        for (self.signature.roles[0..result.count], result.storage[0..result.count]) |role, *input| {
            input.* = switch (role) {
                .input_ids => self.borrow(role, &result.token_shape, tokens),
                .encoder_attention_mask => self.borrow(role, &result.encoder_mask_shape, mask),
                .decoder_attention_mask => blk: {
                    if (self.query_mask.items.len < tokens.len) {
                        const old_len = self.query_mask.items.len;
                        try self.query_mask.resize(self.allocator, tokens.len);
                        @memset(self.query_mask.items[old_len..], 1);
                    }
                    break :blk self.borrow(role, &result.token_shape, self.query_mask.items[0..tokens.len]);
                },
                .encoder_hidden_states => switch (hidden) {
                    .host => |value| .{ .host = value.borrowedView(@tagName(role)) },
                    .resident => hidden,
                },
            };
        }
    }
};

pub const DecoderInputs = struct {
    storage: [4]session_mod.StageInput = undefined,
    host_storage: [4]backends.Tensor = undefined,
    count: usize,
    token_shape: [2]i64,
    encoder_mask_shape: [2]i64,

    pub fn stageInputs(self: *const DecoderInputs) []const session_mod.StageInput {
        return self.storage[0..self.count];
    }

    pub fn hostInputs(self: *DecoderInputs) ![]const backends.Tensor {
        for (self.stageInputs(), self.host_storage[0..self.count]) |value, *host| host.* = switch (value) {
            .host => |tensor| tensor,
            .resident => return error.ExpectedHostEncoder,
        };
        return self.host_storage[0..self.count];
    }
};

pub fn decoderHiddenInput(encoder: *const EncoderOutputs, mask_len: usize) !session_mod.StageInput {
    return switch (encoder.*) {
        .host => |outputs| if (outputs.len == 0) error.NoEncoderOutput else .{ .host = outputs[0] },
        .resident => |*resident| blk: {
            if (resident.values.outputs.len != 1 or resident.shape[1] != mask_len) return error.InvalidInputShape;
            break :blk .{ .resident = .{ .value = resident.values.outputs[0], .backend = resident.values.backend, .shape = &resident.shape } };
        },
    };
}

/// A reading task without an artifact vocabulary hint keeps its existing host
/// output contract. Qualification happens before the encoder is dispatched.
pub fn outputVocabulary(decoder: backends.Session) ?usize {
    const outputs = decoder.outputInfo();
    if (outputs.len != 1 or outputs[0].dtype != .f32 or outputs[0].shape.len != 3 or outputs[0].shape[2] <= 0) return null;
    return std.math.cast(usize, outputs[0].shape[2]);
}

pub fn encodeForFinalRowWithPermit(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit) !EncoderOutputs {
    return encodeOptions(allocator, encoder, decoder, inputs, dispatch, control, supplied, .vocabulary_row);
}

/// Only the final vocabulary row crosses the output boundary. The encoder and
/// every preceding decoder activation remain with their producing backend.
fn finalGreedyToken(allocator: std.mem.Allocator, decoder: backends.Session, encoder: *const EncoderOutputs, tokens: []const i64, mask: []const i64, vocab: usize, control: ?Control) !i32 {
    var plan = try DecoderInputPlan.init(allocator, try decoder.inputInfoForStage(.decoder));
    defer plan.deinit();
    var inputs: DecoderInputs = undefined;
    try plan.bind(&inputs, tokens, mask, try decoderHiddenInput(encoder, mask.len));
    const values = try finalVocabularyRow(allocator, decoder, inputs.stageInputs(), tokens.len, vocab, control, null);
    defer allocator.free(values);
    // Preserve the reference's first-index tie and NaN comparison behavior.
    var best: usize = 0;
    for (values[1..], 1..) |value, index| if (value > values[best]) {
        best = index;
    };
    return std.math.cast(i32, best) orelse error.LogitsSizeMismatch;
}

pub fn finalVocabularyRow(allocator: std.mem.Allocator, decoder: backends.Session, inputs: []const session_mod.StageInput, query_rows: usize, vocab: usize, control: ?Control, dispatch: ?@import("tensor_dispatch.zig").Dispatch) ![]f32 {
    if (query_rows == 0 or vocab == 0) return error.InvalidInputShape;
    var outputs = try @import("tensor_dispatch.zig").runStage(allocator, decoder, inputs, control, dispatch);
    defer outputs.deinit();
    if (outputs.outputs.len != 1) return error.InvalidDecoderOutput;
    return residentVocabularyRow(allocator, decoder, &outputs, 0, query_rows, vocab, control);
}

/// The result retains the provider's admission and tensor custody across the
/// readback. Only the requested row crosses the device boundary.
pub fn residentVocabularyRow(allocator: std.mem.Allocator, decoder: backends.Session, outputs: *const session_mod.ResidentOutputs, logits_index: usize, query_rows: usize, vocab: usize, control: ?Control) ![]f32 {
    if (query_rows == 0 or vocab == 0 or logits_index >= outputs.outputs.len) return error.InvalidDecoderOutput;
    const expected = [_]i64{
        1,
        std.math.cast(i64, query_rows) orelse return error.ResourceLimitExceeded,
        std.math.cast(i64, vocab) orelse return error.ResourceLimitExceeded,
    };
    const logits = outputs.outputs[logits_index];
    var operation_scope: @import("backend_operation.zig").Scope = undefined;
    try @import("backend_operation.zig").enterInto(outputs.backend, null, outputs.backend.operationToken(), .legacy_unknown_frame, &operation_scope);
    errdefer operation_scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    if (try outputs.backend.tensorShapeMatches(logits, &expected)) |matches| {
        if (!matches) return error.InvalidDecoderOutput;
    } else {
        const actual = try outputs.backend.tensorShape(logits, allocator);
        defer allocator.free(actual);
        if (!std.mem.eql(i64, &expected, actual)) return error.InvalidDecoderOutput;
    }
    if (control) |active| try active.check();
    const values = readback: {
        var guard = if (control) |active| try active.enterUninterruptible(decoder.interruption()) else null;
        defer if (guard) |*scope| scope.deinit();
        const slice = outputs.backend.vtable.sliceRows2D orelse return error.UnexpectedResidentStageDecline;
        const row = try slice(outputs.backend.ptr, logits, query_rows - 1, 1, vocab);
        defer outputs.backend.free(row);
        const result = try outputs.backend.toFloat32(row, allocator);
        errdefer allocator.free(result);
        if (result.len != vocab) return error.InvalidDecoderOutput;
        if (control) |active| try active.check();
        break :readback result;
    };
    errdefer allocator.free(values);
    try operation_scope.finish();
    return values;
}

test "encoder decoder resident stages retain hidden state and materialize only the final vocabulary row" {
    const a = std.testing.allocator;
    const build = @import("build_options");
    const imported = @import("../backends/imported_onnx_session.zig");
    const audit_mod = @import("transfer_audit.zig");
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try @import("testing/encoder_decoder.zig").write(a, temp.dir, "encoder.onnx", true);
    try @import("testing/encoder_decoder.zig").write(a, temp.dir, "decoder.onnx", false);
    const encoder_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "encoder.onnx" });
    defer a.free(encoder_path);
    const decoder_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "decoder.onnx" });
    defer a.free(decoder_path);
    var input = try backends.Tensor.initFloat32(a, "features", &.{ 1, 2, 4 }, &.{ -2, 1, 2, -1, 3, -1, 1, 0 });
    defer input.deinit();
    for ([_]backends.BackendType{ .native, .metal, .cuda }) |kind| {
        if (kind == .metal) {
            if (comptime !build.enable_metal) continue;
            if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) continue;
        }
        if (kind == .cuda and comptime !build.enable_cuda) continue;
        var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
        defer std.debug.assert(std.meta.eql(controller.snapshot(), @import("../runtime/tier/memory.zig").AdmissionAmounts{}));
        var encoder = imported.createSessionWithOptions(a, encoder_path, kind, .{ .graph_runtime_strategy = .compiled_preferred }) catch |err| switch (err) {
            error.CudaUnavailable, error.NoCudaDevices => continue,
            else => return err,
        };
        var encoder_open = true;
        defer if (encoder_open) encoder.close();
        var decoder = try imported.createSessionWithOptions(a, decoder_path, kind, .{ .graph_runtime_strategy = .compiled_preferred, .shared_backend_ctx = encoder.shared_compute_context });
        defer decoder.close();
        encoder.run_admission = .{ .controller = &controller, .backend_class = if (kind == .native) .cpu else .gpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false };
        const control = Control{ .native_call_policy = .in_process };
        try std.testing.expectEqual(PlacementReason.resident_pair, residentPairPlacement(encoder, decoder, true));
        const Mismatched = struct {
            fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
                return &.{
                    .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 2 } },
                    .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ 1, 2, 5 } },
                };
            }
        };
        var bad_decoder = decoder;
        var bad_table = decoder.vtable.*;
        bad_table.inputInfo = Mismatched.inputInfo;
        bad_decoder.vtable = &bad_table;
        try std.testing.expectEqual(PlacementReason.unsupported_decoder_signature, residentPairPlacement(encoder, bad_decoder, true));
        const Broker = struct {
            calls: usize = 0,
            encoder_calls: usize = 0,
            fn resident(raw: *anyopaque, _: @import("tensor_dispatch.zig").Task, work: std.mem.Allocator, _: backends.Session, supplied: ?*session_mod.RunPermit, _: ?*std.atomic.Mutex, inputs: []const backends.Tensor, active: ?Control) !?session_mod.ResidentOutputs {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.encoder_calls += 1;
                const permit = supplied orelse return error.InvalidStagePermit;
                try permit.prepareResidentOutputRetirement();
                return permit.runResidentWithControl(inputs, work, active);
            }
            fn host(_: *anyopaque, _: @import("tensor_dispatch.zig").Task, _: std.mem.Allocator, _: backends.Session, _: ?*session_mod.RunPermit, _: ?*std.atomic.Mutex, _: []const backends.Tensor, _: ?Control) ![]backends.Tensor {
                return error.UnexpectedHostDecoder;
            }
            fn stage(raw: *anyopaque, _: @import("tensor_dispatch.zig").Task, work: std.mem.Allocator, target: backends.Session, bindings: []const session_mod.StageInput, active: ?Control) !session_mod.ResidentOutputs {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                for (bindings) |binding| if (binding == .resident) {
                    return (try target.runStageInputsWithControl(bindings, work, active)) orelse error.UnexpectedResidentStageDecline;
                };
                return error.ExpectedResidentEncoder;
            }
        };
        var broker = Broker{};
        const dispatch = @import("tensor_dispatch.zig").Dispatch{ .ptr = &broker, .task = .generate, .run_stage_fn = Broker.stage, .run_resident_fn = Broker.resident, .run_fn = Broker.host };
        var encoded = blk: {
            var audit = audit_mod.Audit{ .forbid_activation_readbacks = true };
            try audit_mod.begin(&audit);
            defer audit_mod.end();
            var permit = try encoder.admitInputs(&.{input}, a, null);
            defer permit.deinit();
            try prepareEncoderPermit(encoder, decoder, null, control, .resident, &permit);
            var result = try encodeWithPermit(a, encoder, decoder, &.{input}, null, control, &permit);
            errdefer result.deinit(a);
            try std.testing.expect(permit.lease == null);
            try std.testing.expect(result.resident.values.resource_lease != null);
            try std.testing.expect(result == .resident);
            try std.testing.expectEqual([3]i64{ 1, 2, 4 }, try residentEncoderShape(std.testing.failing_allocator, encoder.outputInfo(), &result.resident.values));
            const dynamic = [_]backends.TensorInfo{.{ .name = encoder.outputInfo()[0].name, .dtype = .f32, .shape = &.{ 1, -1, 4 } }};
            try std.testing.expectEqual(result.resident.shape, try residentEncoderShape(a, &dynamic, &result.resident.values));
            try std.testing.expectEqual(@as(u64, 0), audit.download_bytes);
            try std.testing.expectEqual(@as(u64, 0), audit.host_access_bytes);
            break :blk result;
        };
        defer encoded.deinit(a);
        {
            var permit = try encoder.admitInputs(&.{input}, a, null);
            defer permit.deinit();
            try prepareEncoderPermit(encoder, decoder, dispatch, control, .resident, &permit);
            try std.testing.expect(permit.output_retirement_shell == null);
            var scheduled = try encodeWithPermit(a, encoder, decoder, &.{input}, dispatch, control, &permit);
            defer scheduled.deinit(a);
            try std.testing.expect(scheduled == .resident);
            try std.testing.expectEqual(@as(usize, 1), broker.encoder_calls);
        }
        encoder.close();
        encoder_open = false;
        for (0..3) |_| {
            var audit = audit_mod.Audit{};
            const token = blk: {
                try audit_mod.begin(&audit);
                defer audit_mod.end();
                break :blk try finalGreedyToken(a, decoder, &encoded, &.{ 0, 1 }, &.{ 1, 1 }, 4, control);
            };
            try std.testing.expectEqual(@as(i32, 1), token);
            if (kind != .native) try std.testing.expectEqual(@as(u64, 4 * @sizeOf(f32)), audit.download_bytes);
        }
        var numerical = try @import("decoder_stage.zig").Decoder.initWithOptions(a, decoder, &encoded, 2, &.{ 1, 1 }, 4, control, .{ .allow_native = false });
        defer numerical.deinit();
        var row = try numerical.stepFullPrefix(&.{ 0, 1 }, dispatch);
        defer row.deinit();
        try std.testing.expectEqual(@as(usize, 1), broker.calls);
        try std.testing.expectEqual(@as(usize, 4), row.logits.len);
        try std.testing.expect(row.logits[1] > row.logits[0]);
        const retained = try encoded.resident.values.backend.toFloat32(encoded.resident.values.outputs[0], a);
        defer a.free(retained);
        try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2, 0, 3, 0, 1, 0 }, retained);
        if (kind == .native) {
            const Case = struct {
                fn run(allocator: std.mem.Allocator, target: backends.Session, values: *const EncoderOutputs) !void {
                    const token = try finalGreedyToken(allocator, target, values, &.{ 0, 1 }, &.{ 1, 1 }, 4, null);
                    try std.testing.expectEqual(@as(i32, 1), token);
                }
            };
            try std.testing.checkAllAllocationFailures(a, Case.run, .{ decoder, @as(*const EncoderOutputs, &encoded) });
        }
    }
}

test "decoder input owner borrows real token hidden and encoder mask storage and owns only query mask" {
    const a = std.testing.allocator;
    const shape = [_]i64{ 1, 2 };
    const signature = [_]backends.TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &shape },
        .{ .name = "encoder_attention_mask", .dtype = .i64, .shape = &shape },
        .{ .name = "decoder_attention_mask", .dtype = .i64, .shape = &shape },
        .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ 1, 2, 4 } },
    };
    const tokens = [_]i64{ 7, 9 };
    const mask = [_]i64{ 1, 0 };
    var hidden = try backends.Tensor.initFloat32(a, "original_hidden", &.{ 1, 2, 4 }, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    defer hidden.deinit();
    const Case = struct {
        fn run(allocator: std.mem.Allocator, infos: []const backends.TensorInfo, ids: []const i64, encoder_mask: []const i64, encoded: backends.Tensor) !void {
            var plan = try DecoderInputPlan.init(allocator, infos);
            defer plan.deinit();
            var inputs: DecoderInputs = undefined;
            try plan.bind(&inputs, ids, encoder_mask, .{ .host = encoded });
            const host = try inputs.hostInputs();
            try std.testing.expect(host[0].data.ptr == std.mem.sliceAsBytes(ids).ptr);
            try std.testing.expect(host[1].data.ptr == std.mem.sliceAsBytes(encoder_mask).ptr);
            try std.testing.expect(host[3].data.ptr == encoded.data.ptr);
            try std.testing.expectEqualSlices(i64, &.{ 1, 1 }, host[2].asInt64());
            try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, host[0].shape);
            try std.testing.expectEqualStrings("encoder_hidden_states", host[3].name);
            const mask_storage = host[2].data.ptr;
            try plan.bind(&inputs, ids[0..1], encoder_mask, .{ .host = encoded });
            const rebound = try inputs.hostInputs();
            try std.testing.expectEqual(mask_storage, rebound[2].data.ptr);
            try std.testing.expectEqualSlices(i64, &.{1}, rebound[2].asInt64());
            try std.testing.expectEqualSlices(i64, &.{ 1, 1 }, rebound[0].shape);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{ @as([]const backends.TensorInfo, &signature), @as([]const i64, &tokens), @as([]const i64, &mask), hidden });
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, hidden.asFloat32());
    try std.testing.expectEqualSlices(i64, &.{ 7, 9 }, &tokens);
    try std.testing.expectEqualSlices(i64, &.{ 1, 0 }, &mask);
}

test "encoder cold preparation shares host fallback and typed dispatcher qualification" {
    const Fixture = struct {
        fn kind(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn run(_: *anyopaque, _: []const backends.Tensor, _: std.mem.Allocator) ![]backends.Tensor {
            return error.UnexpectedRun;
        }
        fn close(_: *anyopaque) void {}
        const table = backends.Session.VTable{ .backend = kind, .inputInfo = info, .outputInfo = info, .run = run, .close = close };
    };
    const Dispatch = struct {
        fn supportsResident(_: *@This()) bool {
            return false;
        }
        fn runResident(_: *@This(), _: std.mem.Allocator, _: backends.Session, _: ?*session_mod.RunPermit, _: ?*std.atomic.Mutex, _: []const backends.Tensor, _: ?Control) !?session_mod.ResidentOutputs {
            return error.UnexpectedRun;
        }
    };
    var marker: u8 = 0;
    const session = backends.Session{ .ptr = &marker, .vtable = &Fixture.table };
    var permit = session_mod.RunPermit{ .session = session, .lease = null };
    defer permit.deinit();
    try prepareEncoderPermit(session, session, null, null, .resident, &permit);
    try std.testing.expect(permit.output_retirement_shell == null);
    var dispatcher = Dispatch{};
    try prepareEncoderPermit(session, session, &dispatcher, null, .incremental, &permit);
    try std.testing.expect(permit.output_retirement_shell == null);
    try std.testing.expect(!encoderRoute(session, session, &dispatcher, null, .incremental).isResident());
    try std.testing.expectEqual(PlacementReason.dispatcher_host_only, encoderRoute(session, session, &dispatcher, null, .incremental).placement);
    try std.testing.expectEqual(PlacementReason.no_source_context, residentPairPlacement(session, session, false));
    try prepareEncoderPermit(session, session, null, null, .vocabulary_row, &permit);
    try std.testing.expect(permit.output_retirement_shell == null);
    try std.testing.expectEqual(PlacementReason.unknown_decoder_vocabulary, encoderRoute(session, session, null, null, .vocabulary_row).placement);
    var diagnostics = @import("diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    publishPlacement(&diagnostics, .native, .cuda, .foreign_context);
    const observed = diagnostics.snapshot().?;
    try std.testing.expectEqual(PlacementReason.foreign_context, observed.report.?.stage_placement.?.reason);
    try std.testing.expect(observed.execution == null);
    try std.testing.expect(observed.report.?.transfers == null);
}
