// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Encoder/decoder stage placement. Tasks keep token acceptance, stopping and
//! text publication; this owner keeps hidden activations in a shared context.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const session_mod = @import("../backends/session.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const EncoderOutputs = union(enum) {
    host: []backends.Tensor,
    resident: struct { values: session_mod.ResidentOutputs, shape: [3]i64 },

    pub fn deinit(self: *EncoderOutputs, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .host => |values| {
                for (values) |*value| value.deinit();
                allocator.free(values);
            },
            .resident => |*value| value.values.deinit(),
        }
    }
};

/// Qualification precedes encoder execution: a rejected pair takes its host
/// reference once. Merged-cache decoders have a separate stateful contract.
pub fn supportsResidentPair(encoder: backends.Session, decoder: backends.Session, controlled: bool) bool {
    const source = encoder.shared_compute_context orelse return false;
    if (decoder.shared_compute_context != source) return false;
    if (encoder.vtable.runResident == null or decoder.vtable.runResidentInputs == null) return false;
    if (controlled and (encoder.vtable.runResidentWithControl == null or decoder.vtable.runResidentInputsWithControl == null)) return false;
    const outputs = encoder.outputInfo();
    if (outputs.len != 1 or outputs[0].dtype != .f32 or outputs[0].shape.len != 3) return false;
    const logits = decoder.outputInfo();
    if (logits.len != 1 or logits[0].dtype != .f32 or logits[0].shape.len != 3) return false;
    var tokens = false;
    var hidden = false;
    for (decoder.inputInfo()) |input| {
        if (std.mem.eql(u8, input.name, "input_ids")) {
            if (tokens or input.dtype != .i64 or input.shape.len != 2) return false;
            tokens = true;
        } else if (std.mem.eql(u8, input.name, "encoder_hidden_states")) {
            if (hidden or input.dtype != .f32 or input.shape.len != 3) return false;
            hidden = true;
        } else if (std.mem.eql(u8, input.name, "encoder_attention_mask")) {
            if (input.dtype != .i64 or input.shape.len != 2) return false;
        } else return false;
    }
    return tokens and hidden;
}

/// The optional synchronous dispatch capability has the existing run method;
/// its task classification stays in its caller and is never stored here.
pub fn encode(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control) !EncoderOutputs {
    return encodeWithPermit(allocator, encoder, decoder, inputs, dispatch, control, null);
}

/// A preprocessing permit may transfer its lease into resident encoder outputs.
/// Host reference execution preserves the caller's existing permit lifetime.
pub fn encodeWithPermit(allocator: std.mem.Allocator, encoder: backends.Session, decoder: backends.Session, inputs: []const backends.Tensor, dispatch: anytype, control: ?Control, supplied: ?*session_mod.RunPermit) !EncoderOutputs {
    if (supplied) |permit| if (permit.session.ptr != encoder.ptr or permit.session.vtable != encoder.vtable) return error.InvalidStagePermit;
    const dispatched = if (comptime @TypeOf(dispatch) == @TypeOf(null)) false else if (comptime @typeInfo(@TypeOf(dispatch)) == .optional) dispatch != null else true;
    if (!dispatched and supportsResidentPair(encoder, decoder, control != null)) {
        var own_permit: ?session_mod.RunPermit = null;
        defer if (own_permit) |*owned| owned.deinit();
        const permit = supplied orelse blk: {
            own_permit = try encoder.admitInputs(inputs, allocator, control);
            break :blk &own_permit.?;
        };
        var values = (try permit.runResidentWithControl(inputs, allocator, control)) orelse return error.UnexpectedResidentStageDecline;
        errdefer values.deinit();
        if (values.resource_lease == null) {
            values.resource_lease = permit.lease;
            permit.lease = null;
        }
        if (values.outputs.len != 1) return error.InvalidEncoderOutput;
        const shape = try values.backend.tensorShape(values.outputs[0], allocator);
        defer allocator.free(shape);
        if (shape.len != 3 or shape[0] != 1 or shape[1] <= 0 or shape[2] <= 0) return error.InvalidEncoderOutput;
        return .{ .resident = .{ .values = values, .shape = .{ shape[0], shape[1], shape[2] } } };
    }
    if (comptime @TypeOf(dispatch) != @TypeOf(null)) {
        if (comptime @typeInfo(@TypeOf(dispatch)) == .optional) {
            if (dispatch) |selected| return .{ .host = try selected.run(allocator, encoder, supplied, null, inputs, control) };
        } else return .{ .host = try dispatch.run(allocator, encoder, supplied, null, inputs, control) };
    }
    return .{ .host = if (supplied) |permit| try permit.runWithControl(inputs, allocator, control) else try encoder.runWithControl(inputs, allocator, control) };
}

/// Only the final vocabulary row crosses the output boundary. The encoder and
/// every preceding decoder activation remain with their producing backend.
pub fn finalGreedyToken(allocator: std.mem.Allocator, decoder: backends.Session, encoder: *const EncoderOutputs, tokens: []const i64, mask: []const i64, vocab: usize, control: ?Control) !i32 {
    const values = try finalVocabularyRow(allocator, decoder, encoder, tokens, mask, vocab, control);
    defer allocator.free(values);
    // Preserve the reference's first-index tie and NaN comparison behavior.
    var best: usize = 0;
    for (values[1..], 1..) |value, index| if (value > values[best]) {
        best = index;
    };
    return std.math.cast(i32, best) orelse error.LogitsSizeMismatch;
}

pub fn finalVocabularyRow(allocator: std.mem.Allocator, decoder: backends.Session, encoder: *const EncoderOutputs, tokens: []const i64, mask: []const i64, vocab: usize, control: ?Control) ![]f32 {
    const resident = switch (encoder.*) {
        .resident => |*value| value,
        .host => return error.ExpectedResidentEncoder,
    };
    if (tokens.len == 0 or vocab == 0 or resident.shape[1] != mask.len) return error.InvalidInputShape;
    const token_shape = [_]i64{ 1, @intCast(tokens.len) };
    const mask_shape = [_]i64{ 1, @intCast(mask.len) };
    var input_storage: [3]session_mod.StageInput = undefined;
    const signature = decoder.inputInfo();
    if (signature.len > input_storage.len) return error.InvalidDecoderInput;
    for (signature, input_storage[0..signature.len]) |info, *input| {
        input.* = if (std.mem.eql(u8, info.name, "input_ids")) .{ .host = .{
            .name = info.name,
            .data = @constCast(std.mem.sliceAsBytes(tokens)),
            .shape = &token_shape,
            .dtype = .i64,
            .allocator = allocator,
            .owns_data = false,
            .owns_shape = false,
        } } else if (std.mem.eql(u8, info.name, "encoder_attention_mask")) .{ .host = .{
            .name = info.name,
            .data = @constCast(std.mem.sliceAsBytes(mask)),
            .shape = &mask_shape,
            .dtype = .i64,
            .allocator = allocator,
            .owns_data = false,
            .owns_shape = false,
        } } else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) .{ .resident = .{
            .value = resident.values.outputs[0],
            .backend = resident.values.backend,
            .shape = &resident.shape,
        } } else return error.InvalidDecoderInput;
    }
    var outputs = (try decoder.runStageInputsWithControl(input_storage[0..signature.len], allocator, control)) orelse return error.UnexpectedResidentStageDecline;
    defer outputs.deinit();
    if (outputs.outputs.len != 1) return error.InvalidDecoderOutput;
    const expected = [_]i64{ 1, @intCast(tokens.len), std.math.cast(i64, vocab) orelse return error.ResourceLimitExceeded };
    if (try outputs.backend.tensorShapeMatches(outputs.outputs[0], &expected)) |matches| {
        if (!matches) return error.InvalidDecoderOutput;
    } else {
        const actual = try outputs.backend.tensorShape(outputs.outputs[0], allocator);
        defer allocator.free(actual);
        if (!std.mem.eql(i64, &expected, actual)) return error.InvalidDecoderOutput;
    }
    if (control) |active| try active.check();
    var guard = if (control) |active| try active.enterUninterruptible(decoder.interruption()) else null;
    defer if (guard) |*scope| scope.deinit();
    const row = try outputs.backend.sliceRows2D(allocator, outputs.outputs[0], tokens.len - 1, 1, vocab);
    defer outputs.backend.free(row);
    const values = try outputs.backend.toFloat32(row, allocator);
    errdefer allocator.free(values);
    if (values.len != vocab) return error.InvalidDecoderOutput;
    if (control) |active| try active.check();
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
        try std.testing.expect(supportsResidentPair(encoder, decoder, true));
        var encoded = blk: {
            var audit = audit_mod.Audit{ .forbid_activation_readbacks = true };
            try audit_mod.begin(&audit);
            defer audit_mod.end();
            var permit = try encoder.admitInputs(&.{input}, a, null);
            defer permit.deinit();
            var result = try encodeWithPermit(a, encoder, decoder, &.{input}, null, control, &permit);
            errdefer result.deinit(a);
            try std.testing.expect(permit.lease == null);
            try std.testing.expect(result.resident.values.resource_lease != null);
            try std.testing.expect(result == .resident);
            try std.testing.expectEqual(@as(u64, 0), audit.download_bytes);
            try std.testing.expectEqual(@as(u64, 0), audit.host_access_bytes);
            break :blk result;
        };
        defer encoded.deinit(a);
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
