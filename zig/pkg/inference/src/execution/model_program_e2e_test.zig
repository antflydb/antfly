// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const platform = @import("antfly_platform");
const Tensor = @import("../backends/tensor.zig").Tensor;
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");

test "model program real EG2 media stages preserve native image composition and embedding" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_EG2_PJRT_QUALIFICATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    const capture = @import("capture.zig");
    const projector = @import("../architectures/gemma4_projector.zig");
    const media = @import("embedding_media_frontend.zig");
    const ops = @import("../ops/ops.zig");
    const Source = struct {
        session: @import("../backends/backends.zig").Session,
        cfg: encoder.Config,
        stage: enum { vision, composition, embedding } = .vision,
        fn expectValues(self: *const @This(), expected: []const f32, actual: []const f32) !void {
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |want, got| {
                try std.testing.expect(std.math.isFinite(want) and std.math.isFinite(got));
                // Intermediate activations can be hundreds in magnitude; the
                // normalized embedding retains its original absolute budget.
                const intermediate = self.stage != .embedding;
                const tolerance: f32 = if (intermediate) 5e-4 + @abs(want) * 1e-4 else 1e-4;
                try std.testing.expectApproxEqAbs(want, got, tolerance);
            }
        }
        fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return factory.getComputeBackend(self.session, allocator);
        }
        fn import(_: *anyopaque, cb: *const ops.ComputeBackend, input: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
            const native: *@import("../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
            return native.importHostTensor(input);
        }
        fn forward(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.stage == .embedding) return media.run(self.cfg, cb, allocator, inputs);
            const value = if (self.stage == .composition) try media.compose(self.cfg, cb, allocator, inputs) else image: {
                const shape = inputs[3].tensor.shape;
                const pixels = try cb.fromFloat32Shape(inputs[3].tensor.asFloat32(), &.{ 1, 3, @intCast(shape[2]), @intCast(shape[3]) });
                defer cb.free(pixels);
                break :image (try projector.encodeEmbeddingGemma2PixelTensorResident(cb, allocator, pixels, @intCast(shape[3]), @intCast(shape[2]))).tensor;
            };
            errdefer cb.free(value);
            const outputs = try allocator.alloc(capture.Output, 1);
            outputs[0] = .{ .name = "result", .value = value };
            return outputs;
        }
    };
    const session = try factory.createNativeSession(a, model);
    defer session.close();
    var source = Source{ .session = session, .cfg = factory.getEmbeddingGemma2Config(session).? };
    var reference = try Source.create(&source, a);
    defer reference.deinit();
    var client = try @import("pjrt").pjrt.Client.initFromEnv(a);
    defer client.deinit();
    var caches = [_]@import("captured_forward.zig").Cache{ .{}, .{} };
    defer for (&caches) |*cache| cache.deinit(a);
    // The small asymmetric grid is the routine regression. Qualification can
    // select the real invoice without changing the binary or model assets.
    var fixture = if (platform.env.getenvBool("ANTFLY_EG2_PJRT_MEDIA_FULL_IMAGE"))
        try projector.prepareEmbeddingGemma2Image(a, @embedFile("../models/testdata/lightonocr3/invoice.png"))
    else small: {
        const pixels = try a.alloc(f32, 3 * 48 * 96);
        for (pixels, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 251)) / 250;
        break :small projector.PreparedImage{ .allocator = a, .pixels = pixels, .geometry = .{ .width = 96, .height = 48, .grid_x = 6, .grid_y = 3, .pooled_x = 2, .pooled_y = 1 } };
    };
    defer fixture.deinit();
    const media_tokens = fixture.geometry.tokenCount();
    // Match the public processor's BOS/image-start/soft-rows/image-end/EOS
    // framing; numerical qualification uses an actual supported image input.
    const sequence = media_tokens + 4;
    const token_ids = try a.alloc(i64, sequence);
    defer a.free(token_ids);
    @memset(token_ids, 258880);
    token_ids[0] = 2;
    token_ids[1] = 255999;
    token_ids[sequence - 2] = 258882;
    token_ids[sequence - 1] = 1;
    const visibility = try a.alloc(i64, sequence);
    defer a.free(visibility);
    @memset(visibility, 1);
    const row_ids = try a.alloc(i64, sequence);
    defer a.free(row_ids);
    for (row_ids, token_ids, 0..) |*row, token, index| row.* = @intCast(if (token == 258880) sequence + index - 2 else index);
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(sequence) }, token_ids);
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", ids.shape, visibility);
    defer mask.deinit();
    var rows = try Tensor.initInt64(a, "media_rows", ids.shape, row_ids);
    defer rows.deinit();
    var pixels = try Tensor.initFloat32(a, "pixel_values", &.{ 1, 3, @intCast(fixture.geometry.height), @intCast(fixture.geometry.width) }, fixture.pixels);
    defer pixels.deinit();
    var width = try Tensor.initInt64(a, "output_dimensions", &.{1}, &.{768});
    defer width.deinit();
    const inputs = [_]capture.Input{ .{ .tensor = ids }, .{ .tensor = mask, .specialize = true }, .{ .tensor = rows, .specialize = true }, .{ .tensor = pixels }, .{ .tensor = width, .specialize = true } };
    for (0..2) |iteration| {
        pixels.asFloat32Mut()[0] = @as(f32, @floatFromInt(iteration)) / 2;
        // Borrowed prepared pixels let the original eager patch extractor
        // remain independent of the compiled reshape/transpose implementation.
        const image = projector.PreparedImage{ .allocator = a, .pixels = pixels.asFloat32Mut(), .geometry = fixture.geometry };
        const projected = try projector.encodePreparedEmbeddingGemma2Image(&reference, a, image);
        defer a.free(projected.embeddings);
        const weight = try reference.getWeight("language_model.embed_tokens.weight");
        defer reference.free(weight);
        const raw = try reference.embeddingLookup(weight, ids.asInt64(), sequence, 512);
        defer reference.free(raw);
        const composed = try reference.toFloat32(raw, a);
        defer a.free(composed);
        for (composed) |*value| value.* *= @sqrt(@as(f32, 512));
        @memcpy(composed[2 * 512 ..][0 .. media_tokens * 512], projected.embeddings);
        const input = try reference.fromFloat32Shape(composed, &.{ @intCast(sequence), 512 });
        defer reference.free(input);
        const pooled = try encoder.forwardPooledEmbeddingsCT(&reference, a, source.cfg, input, mask.asInt64(), 1, sequence, 768);
        defer reference.free(pooled);
        const embedding = try reference.toFloat32(pooled, a);
        defer a.free(embedding);
        for ([_]@TypeOf(source.stage){ .vision, .composition, .embedding }) |stage| {
            source.stage = stage;
            const want = switch (stage) {
                .vision => projected.embeddings,
                .composition => composed,
                .embedding => embedding,
            };
            const eager = try Source.forward(&source, &reference, a, &inputs);
            defer a.free(eager);
            defer for (eager) |value| reference.free(value.value);
            const actual_eager = try reference.toFloat32(eager[0].value, a);
            defer a.free(actual_eager);
            std.debug.print("EG2 media stage={s} iteration={d} eager\n", .{ @tagName(stage), iteration });
            try source.expectValues(want, actual_eager);
            for ([_]@import("precision.zig").Policy{ .f32, .bf16 }, 0..) |precision, precision_index| {
                const cache = &caches[precision_index];
                const forward = capture.Forward{ .context = &source, .fingerprint = 100 + @as(u64, @backingInt(stage)), .precision = precision, .output_names = if (stage == .embedding) &.{"sentence_embedding"} else &.{"result"}, .run = Source.forward };
                const output = try cache.run(a, .{ .context = &source, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client), .pjrt_precision = precision } }, forward, &inputs, a, null);
                defer a.free(output);
                defer for (output) |*value| value.deinit();
                std.debug.print("EG2 media stage={s} iteration={d} PJRT {s}\n", .{ @tagName(stage), iteration, @tagName(precision) });
                if (precision == .f32) {
                    try source.expectValues(want, output[0].asFloat32());
                } else {
                    var dot: f64 = 0;
                    var expected_norm: f64 = 0;
                    var actual_norm: f64 = 0;
                    for (want, output[0].asFloat32()) |expected, actual| {
                        try std.testing.expect(std.math.isFinite(actual));
                        dot += @as(f64, expected) * actual;
                        expected_norm += @as(f64, expected) * expected;
                        actual_norm += @as(f64, actual) * actual;
                    }
                    const cosine = dot / @sqrt(expected_norm * actual_norm);
                    std.debug.print("EG2 media BF16 cosine={d}\n", .{cosine});
                    try std.testing.expect(cosine >= 0.999);
                }
            }
        }
    }
    var peak_constant_pool: usize = 0;
    for (&caches) |*cache| {
        try std.testing.expectEqual(@as(u64, 3), cache.entries.stats.builds);
        for (cache.entries.entries.items) |entry| peak_constant_pool = @max(peak_constant_pool, entry.program.graph.constant_pool.items.len);
    }
    // Attention masks stay symbolic; compiling the full image must not expand
    // them into multi-gigabyte constants or wrap later publication offsets.
    try std.testing.expect(peak_constant_pool < 64 * 1024 * 1024);
    std.debug.print("EG2 captured media peak constant pool={d} bytes\n", .{peak_constant_pool});
}

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

// Exercise both public loading paths against the explicit constructor. F32
// checkpoint parity above is separate: ignoring this policy must fail here.
test "model program real EG2 public loaders preserve explicit BF16 precision" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_EG2_PJRT_QUALIFICATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 3 }, &.{ 2, 7314, 236786 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", ids.shape, &.{ 1, 0, 1 });
    defer mask.deinit();
    var dimensions = try Tensor.initInt64(a, "output_dimensions", &.{1}, &.{128});
    defer dimensions.deinit();
    const inputs = [_]Tensor{ ids, mask, dimensions };
    const expected = reference: {
        const session = try factory.createPjrtSessionWithOptions(a, model, .{ .precision = .bf16, .memory_policy = .cpu });
        defer session.close();
        const outputs = try session.runWithControl(&inputs, a, .{ .native_call_policy = .in_process });
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        break :reference try a.dupe(f32, outputs[0].asFloat32());
    };
    defer a.free(expected);
    {
        // Cross-backend BF16 smoke uses cosine >= .999, as in vLLM's
        // tests/models/utils.py check_embeddings_close. Task-quality release
        // gates remain separate; the F32 coordinate tests above stay strict.
        const reference = try factory.createNativeSession(a, model);
        defer reference.close();
        var backend = try factory.getComputeBackend(reference, a);
        defer backend.deinit();
        const cfg = factory.getEmbeddingGemma2Config(reference) orelse return error.WrongModelArchitecture;
        const pooled = try encoder.forwardPooledCT(&backend, a, cfg, ids.asInt64(), mask.asInt64(), 1, 3, 128);
        defer backend.free(pooled);
        const full = try backend.toFloat32(pooled, a);
        defer a.free(full);
        try std.testing.expectEqual(expected.len, full.len);
        var dot: f64 = 0;
        var norm_bf16: f64 = 0;
        var norm_f32: f64 = 0;
        var largest_difference: f32 = 0;
        for (expected, full) |reduced, original| {
            try std.testing.expect(std.math.isFinite(reduced) and std.math.isFinite(original));
            dot += @as(f64, reduced) * original;
            norm_bf16 += @as(f64, reduced) * reduced;
            norm_f32 += @as(f64, original) * original;
            largest_difference = @max(largest_difference, @abs(reduced - original));
        }
        try std.testing.expectApproxEqAbs(@as(f64, 1), norm_bf16, 1e-5);
        try std.testing.expectApproxEqAbs(@as(f64, 1), norm_f32, 1e-5);
        try std.testing.expect(dot / @sqrt(norm_bf16 * norm_f32) >= 0.999);
        // Comparing public loaders only to each other could let all of them
        // silently ignore BF16. This independent F32 result catches that.
        try std.testing.expect(largest_difference > 1e-6);
    }
    var manager = @import("../backends/backends.zig").SessionManager.init(a);
    manager.required_backend = .pjrt;
    manager.pjrt_memory_policy = .cpu;
    manager.pjrt_precision = .bf16;
    manager.required_backend_invalid = false;
    manager.native_call_policy = .in_process;
    manager.preferred_backends = &.{.pjrt};
    var source = try @import("../architectures/model_definition.zig").LoadedModel.open(a, model);
    defer source.deinit();
    for ([_]bool{ false, true }) |manifest| {
        const session = if (manifest)
            try manager.loadModelSource(.{ .path = model, .kind = .directory, .manifest = &source.manifest }, null)
        else
            try manager.loadModel(model);
        defer session.close();
        const outputs = try session.runWithControl(&inputs, a, .{ .native_call_policy = .in_process });
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        try std.testing.expectEqualStrings("sentence_embedding", outputs[0].name);
        try std.testing.expectEqualSlices(i64, &.{ 1, 128 }, outputs[0].shape);
        for (expected, outputs[0].asFloat32()) |want, actual| {
            try std.testing.expect(std.math.isFinite(actual));
            try std.testing.expectApproxEqAbs(want, actual, 1e-6);
        }
    }
}

test "model program real hybrid decoder rejects uncaptured host arithmetic" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_PJRT_HYBRID_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const session = try factory.createPjrtSessionWithOptions(a, model, .{ .task_override = .generic, .memory_policy = .cpu });
    defer session.close();
    var backend = try factory.getComputeBackend(session, a);
    defer backend.deinit();
    const cfg = factory.getGptConfig(session) orelse return error.WrongModelArchitecture;
    const graph = @import("../graph/root.zig");
    const context = graph.execution.Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg, .compiled_partition_backend = .pjrt, .compiled_attachment_target = .whole_model, .pjrt_client = factory.getPjrtClientPtr(session) };
    var cache = graph.cache.GraphCache.init(a);
    defer cache.deinitWithBackend(&backend);
    const decode = @import("../architectures/gpt.zig").DecodeContext{ .total_sequence_len = 2, .query_sequence_len = 2, .kv_sequence_len = 2 };
    try std.testing.expectError(error.HostReadbackDuringCapture, graph.execution.ensureGraphEntry(&context, &cache, &.{ 1, 2 }, 1, 2, &decode));
    try std.testing.expectEqual(@as(usize, 0), cache.count());
}

test "model program real shared KV decoder serializes positional tables through PJRT" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const model = platform.env.getenv("ANTFLY_PJRT_GEMMA4_QAT_MODEL") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const session = try factory.createNativeSessionWithTaskOverride(a, model, .generic);
    defer session.close();
    var backend = try factory.getComputeBackend(session, a);
    defer backend.deinit();
    const cfg = factory.getGptConfig(session) orelse return error.WrongModelArchitecture;
    const graph = @import("../graph/root.zig");
    const context = graph.execution.Context{ .allocator = a, .cb = &backend, .gpt_config = &cfg, .compiled_partition_backend = .pjrt, .compiled_attachment_target = .whole_model, .pjrt_precision = .bf16 };
    var cache = graph.cache.GraphCache.init(a);
    defer cache.deinitWithBackend(&backend);
    const decode = @import("../architectures/gpt.zig").DecodeContext{ .total_sequence_len = 2, .query_sequence_len = 2, .kv_sequence_len = 2 };
    const entry = graph.execution.ensureGraphEntry(&context, &cache, &.{ 2, 1234 }, 1, 2, &decode) catch |err| {
        std.debug.print("shared KV real checkpoint capture failed: {s}\n", .{@errorName(err)});
        return err;
    };
    const ids = try a.alloc(@import("ml").graph.NodeId, entry.graph.nodeCount());
    defer a.free(ids);
    const mutable = try a.alloc(bool, ids.len);
    defer a.free(mutable);
    @memset(mutable, false);
    for (ids, 0..) |*id, index| {
        id.* = @intCast(index);
        try std.testing.expect(graph.partition.supportsPjrt(entry.graph.node(id.*).op));
    }
    const partition = graph.partition.Partition{ .backend = .pjrt, .node_ids = ids, .external_inputs = &.{} };
    // Shape/HLO qualification only: checkpoint operands remain parameters,
    // so this test neither materializes a provider's copy nor needs a GPU.
    var compiled = try graph.pjrt_compiler.compilePartitionWithOptions(a, &entry.graph, &partition, &backend, .{ .precision = .bf16, .parameter_inputs = true, .mutable_parameters = mutable });
    defer compiled.deinit();
    try std.testing.expect(compiled.hlo_bytes.len > 0);
    try std.testing.expect(compiled.hlo_bytes.len < 64 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 1), compiled.output_node_ids.len);
}
