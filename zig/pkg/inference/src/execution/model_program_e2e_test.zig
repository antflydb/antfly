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
