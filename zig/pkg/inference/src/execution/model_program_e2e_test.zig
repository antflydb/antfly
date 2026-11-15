// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const platform = @import("antfly_platform");
const Tensor = @import("../backends/tensor.zig").Tensor;
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");

test "model program real EG2 PJRT public embedding outputs match native across cache hits" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_EG2_PJRT_QUALIFICATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    const ids = [_][3]i64{ .{ 2, 7314, 236786 }, .{ 2, 564, 30591 } };
    const mask = [_]i64{ 1, 0, 1 };
    var expected: [ids.len][]f32 = undefined;
    var initialized: usize = 0;
    defer for (expected[0..initialized]) |value| a.free(value);
    // Retire the native decoded-weight cache before constructing the compiled
    // model. Both sessions use the same existing immutable checkpoint files.
    {
        const session = try factory.createNativeSession(a, model);
        defer session.close();
        var backend = try factory.getComputeBackend(session, a);
        defer backend.deinit();
        const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.WrongModelArchitecture;
        for (ids, 0..) |values, index| {
            const output = try encoder.forwardPooledCT(&backend, a, cfg, &values, &mask, 1, values.len, 128);
            defer backend.free(output);
            expected[index] = try backend.toFloat32(output, a);
            initialized += 1;
        }
    }
    var manager = @import("../backends/backends.zig").SessionManager.init(a);
    manager.required_backend = .pjrt;
    manager.pjrt_memory_policy = .cpu;
    manager.required_backend_invalid = false;
    manager.native_call_policy = .in_process;
    manager.preferred_backends = &.{.pjrt};
    try manager.validateRequiredBackendPolicy();
    const session = try manager.loadModel(model);
    try std.testing.expectEqual(@import("../backends/backends.zig").BackendType.pjrt, session.backend());
    defer session.close();
    for (ids, expected) |values, want| {
        var input = try Tensor.initInt64(a, "input_ids", &.{ 1, values.len }, &values);
        defer input.deinit();
        var visibility = try Tensor.initInt64(a, "attention_mask", input.shape, &mask);
        defer visibility.deinit();
        var dimensions = try Tensor.initInt64(a, "output_dimensions", &.{1}, &.{128});
        defer dimensions.deinit();
        const outputs = try session.runWithControl(&.{ input, visibility, dimensions }, a, .{ .native_call_policy = .in_process });
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        try std.testing.expectEqual(@as(usize, 1), outputs.len);
        try std.testing.expectEqualStrings("sentence_embedding", outputs[0].name);
        try std.testing.expectEqualSlices(i64, &.{ 1, 128 }, outputs[0].shape);
        const actual = outputs[0].asFloat32();
        try std.testing.expectEqual(want.len, actual.len);
        for (want, actual) |reference, candidate| try std.testing.expectApproxEqAbs(reference, candidate, 1e-4);
    }
}

test "model program real GLiNER25 Deberta backbone PJRT outputs match native" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_GLINER25_MODEL") orelse return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_GLINER25_PJRT_QUALIFICATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    const requests = [_][3]i64{ .{ 1, 2, 3 }, .{ 1, 3, 2 } };
    const masks = [_][3]i64{ .{ 1, 1, 1 }, .{ 1, 0, 1 } };
    var expected: [requests.len][]f32 = undefined;
    var initialized: usize = 0;
    defer for (expected[0..initialized]) |value| a.free(value);
    {
        const session = try factory.createNativeSessionWithTaskOverride(a, model, .generic);
        defer session.close();
        for (requests, 0..) |values, index| {
            var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 3 }, &values);
            defer ids.deinit();
            var visibility = try Tensor.initInt64(a, "attention_mask", ids.shape, &masks[index]);
            defer visibility.deinit();
            const outputs = try session.runWithControl(&.{ ids, visibility }, a, .{ .native_call_policy = .in_process });
            defer a.free(outputs);
            defer for (outputs) |*output| output.deinit();
            expected[index] = try a.dupe(f32, outputs[0].asFloat32());
            initialized += 1;
        }
    }
    const session = try factory.createPjrtSessionWithOptions(a, model, .{ .task_override = .generic, .memory_policy = .cpu });
    defer session.close();
    for (requests, expected, 0..) |values, want, index| {
        var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 3 }, &values);
        defer ids.deinit();
        var visibility = try Tensor.initInt64(a, "attention_mask", ids.shape, &masks[index]);
        defer visibility.deinit();
        const outputs = try session.runWithControl(&.{ ids, visibility }, a, .{ .native_call_policy = .in_process });
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        try std.testing.expectEqualStrings("last_hidden_state", outputs[0].name);
        try std.testing.expectEqual(want.len, outputs[0].asFloat32().len);
        for (want, outputs[0].asFloat32()) |left, right| try std.testing.expectApproxEqAbs(left, right, 1e-4);
    }
}

test "model program real Whisper PJRT encoder and decoder stages match native" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_WHISPER_MODEL") orelse return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_WHISPER_PJRT_QUALIFICATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    var features: Tensor = undefined;
    var encoder_hidden: Tensor = undefined;
    var decoder_ids: [2]Tensor = undefined;
    var decoded: [2][]f32 = undefined;
    var features_live = false;
    var encoder_live = false;
    var initialized: usize = 0;
    defer if (features_live) features.deinit();
    defer if (encoder_live) encoder_hidden.deinit();
    defer for (0..initialized) |index| {
        decoder_ids[index].deinit();
        a.free(decoded[index]);
    };
    {
        const session = try factory.createNativeSession(a, model);
        defer session.close();
        const cfg = factory.getWhisperConfig(session) orelse return error.WrongModelArchitecture;
        const values = try a.alloc(f32, cfg.num_mel_bins * 8);
        defer a.free(values);
        for (values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(index % 13)) - 6)) / 8;
        features = try Tensor.initFloat32(a, "input_features", &.{ 1, cfg.num_mel_bins, 8 }, values);
        features_live = true;
        const outputs = try session.runWithControl(&.{features}, a, .{ .native_call_policy = .in_process });
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        encoder_hidden = try Tensor.initFloat32(a, "encoder_hidden_states", outputs[0].shape, outputs[0].asFloat32());
        encoder_live = true;
        for (0..2) |index| {
            var ids = try Tensor.initInt64(a, "decoder_input_ids", &.{ 1, 3 }, &.{ cfg.decoder_start_token_id, cfg.bos_token_id, if (index == 0) cfg.eos_token_id else cfg.decoder_start_token_id });
            errdefer ids.deinit();
            const logits = try session.runWithControl(&.{ ids, encoder_hidden }, a, .{ .native_call_policy = .in_process });
            defer a.free(logits);
            defer for (logits) |*output| output.deinit();
            const copied = try a.dupe(f32, logits[0].asFloat32());
            decoder_ids[index] = ids;
            decoded[index] = copied;
            initialized += 1;
        }
    }
    var manager = @import("../backends/backends.zig").SessionManager.init(a);
    manager.required_backend = .pjrt;
    manager.pjrt_memory_policy = .cpu;
    manager.required_backend_invalid = false;
    manager.native_call_policy = .in_process;
    const session = loaded: {
        var source = try @import("../architectures/model_definition.zig").LoadedModel.open(a, model);
        defer source.deinit();
        break :loaded try manager.loadModelSource(.{ .path = model, .kind = .directory, .manifest = &source.manifest }, null);
    };
    defer session.close();
    const outputs = try session.runWithControl(&.{features}, a, .{ .native_call_policy = .in_process });
    defer a.free(outputs);
    defer for (outputs) |*output| output.deinit();
    try std.testing.expectEqualStrings("last_hidden_state", outputs[0].name);
    try std.testing.expectEqualSlices(i64, encoder_hidden.shape, outputs[0].shape);
    for (encoder_hidden.asFloat32(), outputs[0].asFloat32()) |left, right| try std.testing.expectApproxEqAbs(left, right, 1e-4);
    for (decoder_ids, decoded) |ids, want| {
        // Feed the same native encoder activation to both decoders to isolate
        // decoder parity from any accumulated encoder rounding difference.
        const logits = try session.runWithControl(&.{ ids, encoder_hidden }, a, .{ .native_call_policy = .in_process });
        defer a.free(logits);
        defer for (logits) |*output| output.deinit();
        try std.testing.expectEqualStrings("logits", logits[0].name);
        try std.testing.expectEqual(want.len, logits[0].asFloat32().len);
        for (want, logits[0].asFloat32()) |left, right| try std.testing.expectApproxEqAbs(left, right, 1e-4);
    }
}
